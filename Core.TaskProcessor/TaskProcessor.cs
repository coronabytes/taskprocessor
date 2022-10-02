using System.Collections.Concurrent;
using System.ComponentModel;
using System.Threading.Tasks.Dataflow;
using Cronos;
using StackExchange.Redis;

namespace Core.TaskProcessor;

public class TaskProcessor : ITaskProcessor
{
    private readonly ActionBlock<TaskContext> _actionBlock;
    private readonly LuaScript _batchScript;
    private readonly TaskProcessorOptions _options;
    private readonly RedisKey[] _queues;
    private readonly ConnectionMultiplexer _redis;
    private readonly LuaScript _scheduleScript;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, TaskContext> _tasks = new();

    public TaskProcessor(TaskProcessorOptions options)
    {
        _options = options;
        _queues = options.Queues.Select(x => (RedisKey)Prefix($"queue:{x}")).ToArray();

        _redis = ConnectionMultiplexer.Connect(options.Redis);

        // batch canceled -> search running tasks and terminate
        _redis.GetSubscriber().Subscribe(Prefix("global:cancel"), (channel, value) =>
        {
            var batchId = (string)value!;

            foreach (var task in _tasks.Values)
                try
                {
                    if (task.BatchId == batchId && !task.IsCancellation)
                        task.Cancel.Cancel();
                }
                catch (Exception)
                {
                    //
                }
        });

        // global run state changed -> pause/resume tasks + schedules
        _redis.GetSubscriber().Subscribe(Prefix("global:run"), (channel, value) => { IsPaused = !(bool)value; });

        _actionBlock = new ActionBlock<TaskContext>(Process, new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = _options.MaxWorkers
        });

        _batchScript = LuaScript.Prepare($@"
local batches = redis.call('zrange', '{Prefix("batches:")}'..@tenant, 0, {long.MaxValue}, 'BYSCORE', 'LIMIT', @skip, @take);

local res = {{}}
for i, v in ipairs(batches) do
  res[i] = redis.call('hgetall', '{Prefix("batch:")}'..v);
end

return res;
");

        _scheduleScript = LuaScript.Prepare($@"
local schedules = redis.call('zrange', '{Prefix("schedules:")}'..@tenant, 0, {long.MaxValue}, 'BYSCORE', 'LIMIT', @skip, @take);

local res = {{}}
for i, v in ipairs(schedules) do
  res[i] = redis.call('hgetall', '{Prefix("schedule:")}'..v);
end

return res;
");

        foreach (var q in _options.Queues)
            // new tasks available -> fetch
            _redis.GetSubscriber().Subscribe(Prefix($"queue:{q}:event"),
                (channel, value) => { FetchAsync().ContinueWith(_ => { }); });
    }

    public Func<TaskContext, Task> Execute { get; set; } = _ => Task.CompletedTask;

    public async Task EnqueueBatchAsync(string queue, string tenant, string batchId, List<TaskData> tasks,
        List<TaskData>? continuations = null, string? scope = null)
    {
        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        tra.SortedSetAddAsync(Prefix($"batches:{tenant}"), batchId,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        tra.SortedSetAddAsync(Prefix("batches:cleanup"), batchId,
            DateTimeOffset.UtcNow.Add(_options.Retention).ToUnixTimeSeconds());

        tra.HashSetAsync(Prefix($"batch:{batchId}"), new[]
        {
            new HashEntry("id", batchId),
            new HashEntry("start", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            new HashEntry("end", 0),
            new HashEntry("state", "go"),
            new HashEntry("done", 0),
            new HashEntry("canceled", 0),
            new HashEntry("failed", 0),
            new HashEntry("duration", 0.0d),
            new HashEntry("tenant", tenant),
            new HashEntry("scope", scope ?? string.Empty),
            new HashEntry("total", tasks.Count),
            new HashEntry("remaining", tasks.Count)
        });

        foreach (var task in tasks)
            tra.HashSetAsync(Prefix($"task:{task.TaskId}"), new[]
            {
                new HashEntry("id", task.TaskId),
                new HashEntry("batch", batchId),
                new HashEntry("data", task.Data),
                new HashEntry("topic", task.Topic),
                new HashEntry("queue", task.Queue ?? queue),
                new HashEntry("retries", task.Retries ?? _options.Retries)
            });

        if (continuations?.Any() == true)
        {
            if (continuations.Count > 100)
                throw new ArgumentException("100 continuations max", nameof(continuations));

            foreach (var continuation in continuations)
            {
                tra.HashSetAsync(Prefix($"task:{continuation.TaskId}"), new[]
                {
                    new HashEntry("id", continuation.TaskId),
                    new HashEntry("batch", batchId),
                    new HashEntry("continuation", true),
                    new HashEntry("data", continuation.Data),
                    new HashEntry("topic", continuation.Topic),
                    new HashEntry("queue", continuation.Queue ?? queue),
                    new HashEntry("retries", continuation.Retries ?? _options.Retries)
                });
                // continuations will not be deleted on completion, since they may run again
                tra.KeyExpireAsync(Prefix($"task:{continuation.TaskId}"), DateTime.UtcNow.Add(_options.Retention));
            }

            tra.ListLeftPushAsync(Prefix($"batch:{batchId}:continuations"),
                continuations.Select(x => (RedisValue)x.TaskId).ToArray());
        }

        foreach (var group in tasks.GroupBy(x => x.Queue))
        {
            var q = group.Key ?? queue;
            tra.ListLeftPushAsync(Prefix($"queue:{q}"), group.Select(x => (RedisValue)x.TaskId).ToArray());
            tra.PublishAsync(Prefix($"queue:{q}:event"), "fetch");
        }

#pragma warning restore CS4014
        await tra.ExecuteAsync().ConfigureAwait(false);
    }

    public async Task<bool> AppendBatchAsync(string queue, string tenant, string batchId, List<TaskData> tasks)
    {
        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        tra.AddCondition(Condition.KeyExists(Prefix($"batch:{batchId}")));
        tra.AddCondition(Condition.HashNotEqual(Prefix($"batch:{batchId}"), "state", "canceled"));
        tra.HashIncrementAsync(Prefix($"batch:{batchId}"), "total", tasks.Count);
        tra.HashIncrementAsync(Prefix($"batch:{batchId}"), "remaining", tasks.Count);
        tra.HashSetAsync(Prefix($"batch:{batchId}"), "state", "go");

        foreach (var task in tasks)
            tra.HashSetAsync(Prefix($"task:{task.TaskId}"), new[]
            {
                new HashEntry("id", task.TaskId),
                new HashEntry("batch", batchId),
                new HashEntry("data", task.Data),
                new HashEntry("topic", task.Topic),
                new HashEntry("queue", task.Queue ?? queue),
                new HashEntry("retries", task.Retries ?? _options.Retries)
            });

        foreach (var group in tasks.GroupBy(x => x.Queue))
        {
            var q = group.Key ?? queue;
            tra.ListLeftPushAsync(Prefix($"queue:{q}"), group.Select(x => (RedisValue)x.TaskId).ToArray());
            tra.PublishAsync(Prefix($"queue:{q}:event"), "fetch");
        }
#pragma warning restore CS4014
        return await tra.ExecuteAsync().ConfigureAwait(false);
    }

    public async Task CancelBatchAsync(string batchId)
    {
        var db = _redis.GetDatabase();

        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        tra.HashSetAsync(Prefix($"batch:{batchId}"), "state", "cancel", When.Always, CommandFlags.FireAndForget);
        tra.PublishAsync(Prefix("global:cancel"), batchId, CommandFlags.FireAndForget);
#pragma warning restore CS4014
        await tra.ExecuteAsync();
    }

    public async Task<bool> FetchAsync()
    {
        if (IsPaused)
            return false;

        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync($@"
if redis.replicate_commands~=nil then 
  redis.replicate_commands() 
end

for i, queue in ipairs(KEYS) do
local taskId = redis.call('rpoplpush', queue, queue.."":checkout"");
if taskId then
  local invis = redis.call('time')[1] + ARGV[1];
  redis.call('zadd', queue.."":pushback"", invis, taskId);
  local taskData = redis.call('hgetall', ""{Prefix("task:")}""..taskId);
  local batchId = redis.call('hget', ""{Prefix("task:")}""..taskId, ""batch"");
  local batchData = redis.call('hgetall', ""{Prefix("batch:")}""..batchId);
  return {{queue, taskId, taskData, batchData}}; 
end;
end
", _queues, new RedisValue[] { (long)_options.Retention.TotalSeconds });

        if (res.IsNull)
            return false;

        var r = (RedisResult[])res!;
        var q = (string)r[0]!;
        var j = (string)r[1]!;
        var taskData = r[2].ToDictionary();
        var batchData = r[3].ToDictionary();

        var isContinuation = taskData.ContainsKey("continuation");
        var state = (string)batchData["state"]!;

        if (state == "go" || (isContinuation && state == "done" ))
        {
            var cts = new CancellationTokenSource();
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, _shutdown.Token);

            var info = new TaskContext
            {
                Processor = this,
                TaskId = j,
                BatchId = (string)taskData["batch"]!,
                Tenant = (string)batchData["tenant"]!,
                Queue = (string?)taskData["queue"],
                Cancel = linkedCts,
                Topic = (string?)taskData["topic"] ?? string.Empty,
                Data = (byte[])taskData["data"]!,
                IsContinuation = isContinuation
            };

            _tasks.TryAdd(info.TaskId, info);
            _actionBlock.Post(info);
        }
        else if (state == "canceled")
        {
            _actionBlock.Post(new TaskContext
            {
                Processor = this,
                TaskId = j,
                BatchId = (string)taskData["batch"]!,
                IsCancellation = true,
                IsContinuation = taskData.ContainsKey("continuation"),
                Queue = q,
            });
        }

        return true;
    }

    public async Task StopAsync()
    {
        _actionBlock.Complete();
        _shutdown.Cancel();
        await _actionBlock.Completion.WaitAsync(TimeSpan.FromSeconds(5));
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token).Token;

        IsPaused = !(bool)await _redis.GetDatabase().StringGetAsync(Prefix("global:run"));

        while (!cancel.IsCancellationRequested)
        {
            if (IsPaused)
            {
                await Task.Delay(_options.PollFrequency, cancel).ContinueWith(_ => { });
                continue;
            }

            if (cancel.IsCancellationRequested)
                return;

            bool gotWork;

            do
            {
                gotWork = await FetchAsync();
            } while (_actionBlock.InputCount < _options.MaxWorkers && gotWork);

            await ExecuteSchedules();
            await CleanUp();

            await Task.Delay(_options.PollFrequency, cancel).ContinueWith(_ => { });
        }
    }

    public Task<bool> ExtendLockAsync(string queue, string taskId, TimeSpan span)
    {
        var db = _redis.GetDatabase();
        return db.SortedSetUpdateAsync(Prefix($"queue:{queue}:pushback"), taskId,
            DateTimeOffset.UtcNow.Add(span).ToUnixTimeSeconds());
    }

    private string Prefix(string s)
    {
        if (!string.IsNullOrWhiteSpace(_options.Prefix))
            return $"{_options.Prefix}:{s}";
        return s;
    }

    private string UnPrefix(string s)
    {
        if (!string.IsNullOrWhiteSpace(_options.Prefix))
            if (s.StartsWith(_options.Prefix + ":"))
                return s.Substring(_options.Prefix.Length + 1);
        return s;
    }

    private async Task Process(TaskContext task)
    {
        // force async task scheduling
        await Task.Yield();

        var db = _redis.GetDatabase();

        if (task.IsCancellation)
        {
            var remaining = Task.FromResult(1L);

            var tra = db.CreateTransaction();
#pragma warning disable CS4014
            tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId);
            tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId);
            if (!task.IsContinuation)
            {
                tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "canceled", flags: CommandFlags.FireAndForget);
                remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");
                tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);
            }
#pragma warning restore CS4014
            await tra.ExecuteAsync().ConfigureAwait(false);

            if (await remaining <= 0 && !task.IsContinuation)
                await CompleteBatchAsync(task, db);

            await _options.OnTaskEnd(task).ConfigureAwait(false);
        }
        else
        {
            var start = DateTimeOffset.UtcNow;

            try
            {
                var retries = await db.HashDecrementAsync(Prefix($"task:{task.TaskId}"), "retries")
                    .ConfigureAwait(false);

                if (retries <= 0)
                {
                    var remaining = Task.FromResult(1L);

                    var tra = db.CreateTransaction();
#pragma warning disable CS4014
                    tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId,
                        flags: CommandFlags.FireAndForget);
                    tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId, CommandFlags.FireAndForget);

                    if (!task.IsContinuation)
                    {
                        tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "failed",
                            flags: CommandFlags.FireAndForget);
                        remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");

                        if (_options.Deadletter)
                            tra.SortedSetAddAsync(Prefix($"queue:{task.Queue}:deadletter"), task.TaskId, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        else
                            tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);

                    }
#pragma warning restore CS4014
                    await tra.ExecuteAsync();

                    if (await remaining <= 0 && !task.IsContinuation)
                        await CompleteBatchAsync(task, db);
                }
                else
                {
                    await _options.OnTaskStart(task).ConfigureAwait(false);

                    await Execute(task).ConfigureAwait(false);

                    var tra = db.CreateTransaction();
#pragma warning disable CS4014
                    tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId,
                        flags: CommandFlags.FireAndForget);

                    var remaining = Task.FromResult(1L);

                    if (!task.IsContinuation)
                    {
                        tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "done",
                            flags: CommandFlags.FireAndForget);
                        remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");
                        tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "duration",
                            (DateTimeOffset.UtcNow - start).TotalSeconds, CommandFlags.FireAndForget);
                    }

                    tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId);

                    tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);
#pragma warning restore CS4014
                    await tra.ExecuteAsync().ConfigureAwait(false);

                    if (await remaining <= 0 && !task.IsContinuation)
                        await CompleteBatchAsync(task, db);

                    await _options.OnTaskEnd(task).ConfigureAwait(false);
                }
            }
            catch
            {
                var tra = db.CreateTransaction();
#pragma warning disable CS4014
                tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId, flags: CommandFlags.FireAndForget);
                tra.ListLeftPushAsync(Prefix($"queue:{task.Queue}"), task.TaskId, flags: CommandFlags.FireAndForget);
                tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId, CommandFlags.FireAndForget);

                if (!task.IsContinuation)
                {
                    tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "duration",
                        (DateTimeOffset.UtcNow - start).TotalSeconds, CommandFlags.FireAndForget);
                }
#pragma warning restore CS4014
                await tra.ExecuteAsync().ConfigureAwait(false);
            }
            finally
            {
                _tasks.TryRemove(task.TaskId, out _);
            }
        }


        // poll next task now
        if (!_shutdown.IsCancellationRequested)
            await FetchAsync();
    }

    private async Task CompleteBatchAsync(TaskContext task, IDatabase db)
    {
        var tra2 = db.CreateTransaction();
#pragma warning disable CS4014
        tra2.AddCondition(Condition.HashEqual(Prefix($"batch:{task.BatchId}"), "remaining", 0));
        tra2.AddCondition(Condition.HashNotEqual(Prefix($"batch:{task.BatchId}"), "state", "done"));
        tra2.HashSetAsync(Prefix($"batch:{task.BatchId}"), new[]
        {
            new HashEntry("end", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            new HashEntry("state", "done")
        });
        // push continuations into their queues
        tra2.ScriptEvaluateAsync($@"
if redis.replicate_commands~=nil then 
  redis.replicate_commands() 
end

local continuations = redis.call('lrange', KEYS[1], 0, 100);

for i, taskId in ipairs(continuations) do
  local q = redis.call('hget', '{Prefix("task:")}'..taskId, 'queue');
  redis.call('lpush', '{Prefix("queue:")}'..q, taskId);
end;
", new RedisKey[] { Prefix($"batch:{task.BatchId}:continuations") });
#pragma warning restore CS4014
        await tra2.ExecuteAsync().ConfigureAwait(false);
    }

    public async Task CleanUp()
    {
        var db = _redis.GetDatabase();

        await db.ScriptEvaluateAsync($@"
if redis.replicate_commands~=nil then 
  redis.replicate_commands() 
end

local t = redis.call('time')[1];

for i, v in ipairs(KEYS) do
  local r = redis.call('zrange', v.."":pushback"", 0, t, 'BYSCORE', 'LIMIT', 0, 100);
  for j, w in ipairs(r) do
    redis.call('lpush', v, w);
    redis.call('zrem', v.."":pushback"", w);
    redis.call('lrem', v.."":checkout"", 0, w);
  end;
end;

local batches = redis.call('zrange', ""{Prefix("batches:cleanup")}"", 0, t, 'BYSCORE', 'LIMIT', 0, 100);
for k, batchId in ipairs(batches) do
  local tenant = redis.call('hget', ""{Prefix("batch:")}""..batchId, ""tenant"");
  redis.call('zrem', ""{Prefix("batches:cleanup")}"", batchId);
  redis.call('zrem', ""{Prefix("batches:")}""..tenant, batchId);
  redis.call('del', ""{Prefix("batch:")}""..batchId);
  redis.call('del', ""{Prefix("batch:")}""..batchId.."":continuations"");
end;
", _queues);
    }

    public async Task<long> RequeueDeadletterAsync(string queue, int? retries = null, long? count = null)
    {
        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync($@"
if redis.replicate_commands~=nil then 
  redis.replicate_commands() 
end

local t = redis.call('time')[1];
local taskIds = redis.call('zrange', KEYS[2], 0, t, 'BYSCORE', 'LIMIT', 0, ARGV[1]);

for j, taskId in ipairs(taskIds) do
  redis.call('hset', '{Prefix("task:")}'..taskId, 'retries', ARGV[2]);  
  redis.call('lpush', KEYS[1], taskId);
  redis.call('zrem', KEYS[2], taskId);
end;

return #(taskIds);
", new RedisKey[]
        {
            Prefix($"queue:{queue}"), Prefix($"queue:{queue}:deadletter")
        }, new RedisValue[]
        {
            count ?? 100, retries ?? _options.Retries
        }).ConfigureAwait(false);

        return (long)res;
    }

    public async Task<long> DiscardDeadletterAsync(string queue, long? count = null)
    {
        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync($@"
if redis.replicate_commands~=nil then 
  redis.replicate_commands() 
end

local t = redis.call('time')[1];
local taskIds = redis.call('zrange', KEYS[1], 0, t, 'BYSCORE', 'LIMIT', 0, ARGV[1]);

for j, taskId in ipairs(taskIds) do
  redis.call('zrem', KEYS[1], taskId);
  redis.call('del', '{Prefix("task:")}'..taskId);  
end;

return #(taskIds);
", new RedisKey[]
        {
            Prefix($"queue:{queue}:deadletter")
        }, new RedisValue[]
        {
            count ?? 100
        }).ConfigureAwait(false);

        return (long)res;
    }

    #region Control

    public bool IsPaused { get; private set; } = true;

    public async Task Pause()
    {
        await _redis.GetDatabase().StringSetAsync(Prefix("global:run"), 0);
        await _redis.GetSubscriber().PublishAsync(Prefix("global:run"), false);
    }

    public async Task Resume()
    {
        await _redis.GetDatabase().StringSetAsync(Prefix("global:run"), 1);
        await _redis.GetSubscriber().PublishAsync(Prefix("global:run"), true);
    }

    #endregion

    #region Schedule

    public async Task UpsertScheduleAsync(string globalUniqueId, string tenant, string scope, string topic, byte[] data,
        string queue,
        string cron, string? timeZoneId = null, bool unique = false)
    {
        var cronEx = CronExpression.Parse(cron);
        var now = DateTime.UtcNow;

        var tz = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId ?? "Etc/UTC");
        var next = (DateTimeOffset?)cronEx.GetNextOccurrence(now, tz);

        // sanity check?
        if (next.HasValue)
            if ((next.Value - now).TotalMinutes < 1.0d)
                throw new ArgumentException("cron minimum of one minute", nameof(cron));

        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        tra.HashSetAsync(Prefix($"schedule:{globalUniqueId}"), new[]
        {
            new HashEntry("id", globalUniqueId),
            new HashEntry("timezone", timeZoneId),
            new HashEntry("data", data),
            new HashEntry("topic", topic),
            new HashEntry("queue", queue),
            new HashEntry("scope", scope),
            new HashEntry("tenant", tenant),
            new HashEntry("cron", cron),
            new HashEntry("next", next!.Value.ToUnixTimeSeconds()),
            new HashEntry("unique", unique ? globalUniqueId : string.Empty)
        }).ConfigureAwait(false);
        tra.SortedSetAddAsync(Prefix("schedules"), globalUniqueId, next!.Value.ToUnixTimeSeconds());
        tra.SortedSetAddAsync(Prefix($"schedules:{tenant}"), globalUniqueId, 0);
#pragma warning restore CS4014
        await tra.ExecuteAsync().ConfigureAwait(false);
    }

    public async Task<bool> CancelScheduleAsync(string globalUniqueId, string tenant)
    {
        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        var ok = tra.SortedSetRemoveAsync(Prefix("schedules"), globalUniqueId);
        tra.SortedSetRemoveAsync(Prefix($"schedules:{tenant}"), globalUniqueId, CommandFlags.FireAndForget);
        tra.KeyDeleteAsync(Prefix($"schedule:{globalUniqueId}"), CommandFlags.FireAndForget);
#pragma warning restore CS4014
        await tra.ExecuteAsync().ConfigureAwait(false);

        return await ok;
    }

    public async Task<int> ExecuteSchedules()
    {
        var db = _redis.GetDatabase();

        var lockTaken = await db.LockTakeAsync(Prefix("scheduler-lock"), Environment.MachineName,
            TimeSpan.FromSeconds(20)).ConfigureAwait(false);

        if (!lockTaken)
            return 0;

        try
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            
            var tasks = await db.SortedSetRangeByScoreAsync(Prefix("schedules"), 0, now, take: 100)
                .ConfigureAwait(false);

            foreach (var rid in tasks)
                try
                {
                    var id = (string)rid!;
                    var data = (await db.HashGetAllAsync(Prefix($"schedule:{id}"))).ToDictionary(x => x.Name,
                        x => x.Value);

                    data.TryGetValue("cron", out var cronEx);
                    data.TryGetValue("timezone", out var tzv);
                    data.TryGetValue("next", out var nextOffset);
                    data.TryGetValue("queue", out var queue);
                    data.TryGetValue("data", out var payload);
                    data.TryGetValue("unique", out var unique);
                    data.TryGetValue("tenant", out var tenant);

                    var tra = db.CreateTransaction();
#pragma warning disable CS4014

                    var newTaskId = (string?)unique ?? Guid.NewGuid().ToString("N");

                    // when unique and task data exists -> abort transaction
                    if ((string?)unique != null)
                        tra.AddCondition(Condition.KeyNotExists(Prefix($"task:{newTaskId}")));

                    tra.HashSetAsync(Prefix($"batch:{newTaskId}"), new[]
                    {
                        new HashEntry("start", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                        new HashEntry("end", 0),
                        new HashEntry("state", "go"),
                        new HashEntry("done", 0),
                        new HashEntry("canceled", 0),
                        new HashEntry("failed", 0),
                        new HashEntry("duration", 0.0d),
                        new HashEntry("tenant", tenant),
                        new HashEntry("total", 1),
                        new HashEntry("remaining", 1),
                        new HashEntry("continuations", string.Empty)
                    }, CommandFlags.FireAndForget);

                    tra.SortedSetAddAsync(Prefix($"batches:{(string)tenant!}"), newTaskId,
                        DateTimeOffset.UtcNow.ToUnixTimeSeconds(), CommandFlags.FireAndForget);
                    tra.SortedSetAddAsync(Prefix("batches:cleanup"), newTaskId,
                        DateTimeOffset.UtcNow.Add(_options.Retention).ToUnixTimeSeconds(), CommandFlags.FireAndForget);

                    tra.HashSetAsync(Prefix($"task:{newTaskId}"), new[]
                    {
                        new HashEntry("batch", newTaskId),
                        new HashEntry("schedule", id),
                        new HashEntry("data", payload),
                        new HashEntry("queue", queue),
                        new HashEntry("retries", _options.Retries)
                    }, CommandFlags.FireAndForget);

                    // enqueue
                    tra.ListLeftPushAsync(Prefix($"queue:{queue}"), newTaskId);
#pragma warning restore CS4014

                    // when recurring
                    if (!string.IsNullOrWhiteSpace(cronEx))
                    {
                        var cron = CronExpression.Parse(cronEx);
                        var tz = TimeZoneInfo.FindSystemTimeZoneById((string?)tzv ?? "Etc/UTC");
                        //var n = DateTimeOffset.FromUnixTimeSeconds((long)nextOffset);

                        // TODO: ???
                        var n = DateTimeOffset.UtcNow;

                        var next = cron.GetNextOccurrence(n, tz);


#pragma warning disable CS4014
                        if (next.HasValue)
                        {
                            tra.HashSetAsync(Prefix($"schedule:{id}"), "next", next.Value.ToUnixTimeSeconds());
                            tra.SortedSetAddAsync(Prefix("schedules"), id, next.Value.ToUnixTimeSeconds());
                        }
                        else
                        {
                            db.SortedSetRemoveAsync(Prefix("schedules"), id, CommandFlags.FireAndForget);
                            db.KeyDeleteAsync(Prefix($"schedule:{id}"), CommandFlags.FireAndForget);
                        }
#pragma warning restore CS4014
                    }
                    else
                    {
#pragma warning disable CS4014
                        db.SortedSetRemoveAsync(Prefix("schedules"), id, CommandFlags.FireAndForget);
                        db.KeyDeleteAsync(Prefix($"schedule:{id}"), CommandFlags.FireAndForget);
#pragma warning restore CS4014
                    }

                    var executed = await tra.ExecuteAsync();

                    if ((string?)unique != null && !executed)
                        // transaction aborted -> update schedule
                        if (!string.IsNullOrWhiteSpace(cronEx))
                        {
                            var cron = CronExpression.Parse(cronEx);
                            var tz = TimeZoneInfo.FindSystemTimeZoneById((string?)tzv ?? "Etc/UTC");

                            //var n = DateTimeOffset.FromUnixTimeSeconds((long)nextOffset);
                            // TODO: ???
                            var n = DateTimeOffset.UtcNow;

                            var next = cron.GetNextOccurrence(n, tz);

                            if (next.HasValue)
                            {
                                var tra2 = db.CreateTransaction();
#pragma warning disable CS4014
                                tra2.HashSetAsync(Prefix($"schedule:{id}"), "next", next.Value.ToUnixTimeSeconds());
                                tra2.SortedSetAddAsync(Prefix("schedules"), id,
                                    next.Value.ToUnixTimeSeconds());
#pragma warning restore CS4014
                                await tra2.ExecuteAsync();
                            }
                        }
                }
                catch (Exception)
                {
                    //_logger.LogError(ex, ex.Message);
                }

            return tasks.Length;
        }
        finally
        {
            await db.LockReleaseAsync(Prefix("scheduler-lock"), Environment.MachineName).ConfigureAwait(false);
        }
    }

    #endregion

    #region Stats

    public async Task<BatchInfo> GetBatch(string batchId)
    {
        var db = _redis.GetDatabase();

        var res = await db.HashGetAllAsync(Prefix($"batch:{batchId}")).ConfigureAwait(false);

        if (res.Length == 0)
            return new BatchInfo();

        var batch = res.ToDictionary();

        var end = (long)batch["end"];

        return new BatchInfo
        {
            Id = batch["id"]!,
            Start = DateTimeOffset.FromUnixTimeSeconds((long)batch["start"]).DateTime,
            End = end <= 0 ? null : DateTimeOffset.FromUnixTimeSeconds(end).DateTime,
            Done = (long)batch["done"],
            Canceled = (long)batch["canceled"],
            Failed = (long)batch["failed"],
            Total = (long)batch["total"],
            State = batch["state"]!,
            Duration = (double)batch["duration"]!,
            Remaining = (long)batch["remaining"]!
        };
    }

    public async Task<ICollection<BatchInfo>> GetBatchesAsync(string tenant, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync(_batchScript, new
        {
            tenant,
            skip,
            take
        }, CommandFlags.PreferReplica);

        var list = new List<BatchInfo>();

        foreach (var batchResult in ((RedisResult[])res)!)
        {
            var batch = batchResult.ToDictionary();
            var end = (long)batch["end"];
            list.Add(new BatchInfo
            {
                Id = (string)batch["id"]!,
                Start = DateTimeOffset.FromUnixTimeSeconds((long)batch["start"]).DateTime,
                End = end <= 0 ? null : DateTimeOffset.FromUnixTimeSeconds(end).DateTime,
                Done = (long)batch["done"],
                Canceled = (long)batch["canceled"],
                Failed = (long)batch["failed"],
                Total = (long)batch["total"],
                State = (string)batch["state"]!,
                Duration = (double)batch["duration"]!,
                Remaining = (long)batch["remaining"]!
            });
        }

        return list;
    }

    public async Task<ICollection<QueueInfo>> GetQueuesAsync()
    {
        var db = _redis.GetDatabase();
        var list = new List<QueueInfo>();

        foreach (var q in _queues)
            list.Add(new QueueInfo
            {
                Name = UnPrefix(q!).Replace("queue:", string.Empty),
                Length = await db.ListLengthAsync(q),
                Checkout = await db.SortedSetLengthAsync($"{q}:checkout")
            });

        return list;
    }

    public async Task<QueueInfo> GetQueueAsync(string name)
    {
        var db = _redis.GetDatabase();

        return new QueueInfo
        {
            Name = name,
            Length = await db.ListLengthAsync(Prefix($"queue:{name}")),
            Checkout = await db.SortedSetLengthAsync(Prefix($"queue:{name}:checkout"))
        };
    }

    public async Task<ICollection<TaskData>> GetTasksInQueueAsync(string queue, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var taskIds = await db.ListRangeAsync(Prefix(queue), skip, skip + take,
            CommandFlags.PreferReplica).ConfigureAwait(false);

        var list = new List<TaskData>(taskIds.Length);
        foreach (var taskId in taskIds)
        {
            var hashValues = await db.HashGetAllAsync(Prefix($"task:{taskId}"), CommandFlags.PreferReplica)
                .ConfigureAwait(false);
            var taskData = hashValues.ToDictionary();

            list.Add(new TaskData
            {
                Topic = taskData["topic"]!,
                TaskId = taskId!,
                Tenant = taskData["tenant"]!,
                Data = null!
            });
        }

        return list;
    }

    public async Task<ICollection<ScheduleInfo>> GetSchedules(string tenant, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync(_scheduleScript, new
        {
            tenant,
            skip,
            take
        }, CommandFlags.PreferReplica);

        var list = new List<ScheduleInfo>();

        foreach (var scheduleResult in ((RedisResult[])res)!)
        {
            var schedule = scheduleResult.ToDictionary();
            list.Add(new ScheduleInfo
            {
                Scope = (string)schedule["scope"]!,
                Cron = (string)schedule["cron"]!,
                Timezone = (string)schedule["timezone"]!,
                Next = DateTimeOffset.FromUnixTimeSeconds((long)schedule["next"]).DateTime
            });
        }

        return list;
    }

    #endregion
}