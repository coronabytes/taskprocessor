using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using Cronos;
using StackExchange.Redis;

namespace Core.TaskProcessor;

public class TaskProcessor : ITaskProcessor
{
    private readonly ActionBlock<TaskContext> _actionBlock;
    private readonly TaskProcessorOptions _options;
    private readonly RedisKey[] _queues;
    private readonly ConnectionMultiplexer _redis;
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

        foreach (var q in _options.Queues)
            // new tasks available -> fetch
            _redis.GetSubscriber().Subscribe(Prefix($"queue:{q}:event"),
                (channel, value) => { FetchAsync().ContinueWith(_ => { }); });
    }

    public Func<TaskContext, Task> Execute { get; set; } = _ => Task.CompletedTask;

    public async Task<string> EnqueueBatchAsync(string queue, string tenant, List<TaskData> tasks,
        List<TaskData>? continuations = null, string? scope = null)
    {
        var batchId = Guid.NewGuid().ToString("D");

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

        var push = new Dictionary<string, List<RedisValue>>();

        foreach (var task in tasks)
        {
            var taskId = Guid.NewGuid().ToString("D");

            var q = task.Queue ?? queue;

            if (!push.ContainsKey(q))
                push.Add(q, new List<RedisValue>());

            push[q].Add(taskId);

            tra.HashSetAsync(Prefix($"task:{taskId}"), new[]
            {
                new HashEntry("id", taskId),
                new HashEntry("batch", batchId),
                new HashEntry("tenant", tenant),
                new HashEntry("data", task.Data),
                new HashEntry("topic", task.Topic),
                new HashEntry("queue", task.Queue ?? queue),
                new HashEntry("retries", task.Retries ?? _options.Retries)
            });
        }

        if (continuations?.Any() == true)
        {
            if (continuations.Count > 100)
                throw new ArgumentException("100 continuations max", nameof(continuations));

            var continuationIds = new List<RedisValue>();

            foreach (var continuation in continuations)
            {
                var taskId = Guid.NewGuid().ToString("D");
                continuationIds.Add(taskId);

                tra.HashSetAsync(Prefix($"task:{taskId}"), new[]
                {
                    new HashEntry("id", taskId),
                    new HashEntry("batch", batchId),
                    new HashEntry("tenant", tenant),
                    new HashEntry("continuation", true),
                    new HashEntry("data", continuation.Data),
                    new HashEntry("topic", continuation.Topic),
                    new HashEntry("queue", continuation.Queue ?? queue),
                    new HashEntry("retries", continuation.Retries ?? _options.Retries)
                });
                // continuations will not be deleted on completion, since they may run again
                tra.KeyExpireAsync(Prefix($"task:{taskId}"), DateTime.UtcNow.Add(_options.Retention));
            }

            tra.ListLeftPushAsync(Prefix($"batch:{batchId}:continuations"), continuationIds.ToArray());
        }

        foreach (var q in push)
        {
            tra.ListLeftPushAsync(Prefix($"queue:{q.Key}"), q.Value.ToArray());
            tra.PublishAsync(Prefix($"queue:{q.Key}:event"), "fetch");
        }

#pragma warning restore CS4014
        await tra.ExecuteAsync().ConfigureAwait(false);

        return batchId;
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

        var push = new Dictionary<string, List<RedisValue>>();

        foreach (var task in tasks)
        {
            var taskId = Guid.NewGuid().ToString("D");
            var q = task.Queue ?? queue;

            if (!push.ContainsKey(q))
                push.Add(q, new List<RedisValue>());

            push[q].Add(taskId);

            tra.HashSetAsync(Prefix($"task:{taskId}"), new[]
            {
                new HashEntry("id", taskId),
                new HashEntry("batch", batchId),
                new HashEntry("tenant", tenant),
                new HashEntry("data", task.Data),
                new HashEntry("topic", task.Topic),
                new HashEntry("queue", task.Queue ?? queue),
                new HashEntry("retries", task.Retries ?? _options.Retries)
            });
        }

        foreach (var q in push)
        {
            tra.ListLeftPushAsync(Prefix($"queue:{q.Key}"), q.Value.ToArray());
            tra.PublishAsync(Prefix($"queue:{q.Key}:event"), "fetch");
        }
#pragma warning restore CS4014
        return await tra.ExecuteAsync().ConfigureAwait(false);
    }

    public async Task<bool> CancelBatchAsync(string batchId)
    {
        var db = _redis.GetDatabase();

        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        var ok = tra.HashSetAsync(Prefix($"batch:{batchId}"), "state", "cancel", When.Exists);
        tra.PublishAsync(Prefix("global:cancel"), batchId, CommandFlags.FireAndForget);
#pragma warning restore CS4014
        await tra.ExecuteAsync();

        return await ok;
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

        var isContinuation = taskData.ContainsKey("continuation");
        var isScheduled = taskData.ContainsKey("schedule");
        var isBatch = taskData.ContainsKey("batch");

        if (isBatch)
        {
            var batchData = r[3].ToDictionary();
            var state = (string)batchData["state"]!;

            if (state == "go" || (isContinuation && state == "done"))
            {
                var cts = new CancellationTokenSource();
                var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, _shutdown.Token);

                var info = new TaskContext
                {
                    Processor = this,
                    TaskId = j,
                    BatchId = (string)taskData["batch"]!,
                    Tenant = (string)taskData["tenant"]!,
                    Queue = (string?)taskData["queue"],
                    Cancel = linkedCts,
                    Topic = (string?)taskData["topic"] ?? string.Empty,
                    Data = (byte[])taskData["data"]!,
                    IsContinuation = isContinuation,
                    IsCancellation = false,
                    Retries = (int?)taskData["retries"],
                    ScheduleId = null
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
                    IsContinuation = isContinuation,
                    Queue = q,
                    ScheduleId = null
                });
            }
        }
        else if (isScheduled)
        {
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);

            var info = new TaskContext
            {
                Processor = this,
                TaskId = j,
                ScheduleId = (string)taskData["schedule"]!,
                Tenant = (string)taskData["tenant"]!,
                Queue = (string?)taskData["queue"],
                Cancel = linkedCts,
                Topic = (string?)taskData["topic"] ?? string.Empty,
                Data = (byte[])taskData["data"]!,
                BatchId = null,
                Retries = (int?)taskData["retries"],
                IsCancellation = false,
                IsContinuation = false
            };

            _tasks.TryAdd(info.TaskId, info);
            _actionBlock.Post(info);
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

            await ExecuteSchedulesAsync();
            await CleanUpAsync();

            await Task.Delay(_options.PollFrequency, cancel).ContinueWith(_ => { });
        }
    }

    public Task<bool> ExtendLockAsync(string queue, string taskId, TimeSpan span)
    {
        var db = _redis.GetDatabase();
        return db.SortedSetUpdateAsync(Prefix($"queue:{queue}:pushback"), taskId,
            DateTimeOffset.UtcNow.Add(span).ToUnixTimeSeconds());
    }

    public async Task CleanUpAsync()
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
                    tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId,
                        CommandFlags.FireAndForget);

                    if (!task.IsContinuation)
                    {
                        tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "failed",
                            flags: CommandFlags.FireAndForget);
                        remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");

                        if (_options.Deadletter)
                            tra.SortedSetAddAsync(Prefix($"queue:{task.Queue}:deadletter"), task.TaskId,
                                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        else
                            tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);
                    }
#pragma warning restore CS4014
                    await tra.ExecuteAsync().ConfigureAwait(false);

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
                tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId,
                    flags: CommandFlags.FireAndForget);
                tra.ListLeftPushAsync(Prefix($"queue:{task.Queue}"), task.TaskId, flags: CommandFlags.FireAndForget);
                tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId,
                    CommandFlags.FireAndForget);

                if (!task.IsContinuation)
                    tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "duration",
                        (DateTimeOffset.UtcNow - start).TotalSeconds, CommandFlags.FireAndForget);
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

    public async Task PauseAsync()
    {
        await _redis.GetDatabase().StringSetAsync(Prefix("global:run"), 0);
        await _redis.GetSubscriber().PublishAsync(Prefix("global:run"), false);
    }

    public async Task ResumeAsync()
    {
        await _redis.GetDatabase().StringSetAsync(Prefix("global:run"), 1);
        await _redis.GetSubscriber().PublishAsync(Prefix("global:run"), true);
    }

    #endregion

    #region Schedule

    public async Task UpsertScheduleAsync(ScheduleData schedule, TaskData task)
    {
        if (task.Queue == null)
            throw new ArgumentNullException(nameof(task.Queue));

        var cronEx = CronExpression.Parse(schedule.Cron);
        var now = DateTime.UtcNow;

        var tz = TimeZoneInfo.FindSystemTimeZoneById(schedule.Timezone ?? "Etc/UTC");
        var next = (DateTimeOffset?)cronEx.GetNextOccurrence(now, tz);

        // sanity check?
        if (next.HasValue)
            if ((next.Value - now).TotalMinutes < 1.0d)
                throw new ArgumentException("cron minimum of one minute", nameof(schedule.Cron));

        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        tra.HashSetAsync(Prefix($"schedule:{schedule.ScheduleId}"), new[]
        {
            new HashEntry("id", schedule.ScheduleId),
            new HashEntry("timezone", schedule.Timezone),
            new HashEntry("data", task.Data),
            new HashEntry("topic", task.Topic),
            new HashEntry("queue", task.Queue ?? "default"),
            new HashEntry("scope", schedule.Scope),
            new HashEntry("tenant", schedule.Tenant),
            new HashEntry("retries", task.Retries ?? _options.Retries),
            new HashEntry("cron", schedule.Cron),
            new HashEntry("next", next!.Value.ToUnixTimeSeconds()),
            new HashEntry("unique", schedule.Unique)
        }).ConfigureAwait(false);
        tra.SortedSetAddAsync(Prefix("schedules"), schedule.ScheduleId, next!.Value.ToUnixTimeSeconds());
        tra.SortedSetAddAsync(Prefix($"schedules:{schedule.Tenant}"), schedule.ScheduleId, 0);
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

    public async Task<bool> TriggerScheduleAsync(string id)
    {
        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();

        var info = await TriggerScheduleInternal(id, db, tra).ConfigureAwait(false);

        if (info == null)
            return false;

        return await tra.ExecuteAsync().ConfigureAwait(false);
    }

    // not cluster safe - queue and task on same shard???
    private async Task<ScheduleInfo?> TriggerScheduleInternal(string id, IDatabase db, ITransaction tra)
    {
        var data = (await db.HashGetAllAsync(Prefix($"schedule:{id}"))).ToDictionary(x => x.Name,
            x => x.Value);

        if (data.Count == 0)
            return null;

        data.TryGetValue("cron", out var cronEx);
        data.TryGetValue("timezone", out var tzv);
        data.TryGetValue("next", out var nextOffset);
        data.TryGetValue("queue", out var queue);
        data.TryGetValue("data", out var payload);
        data.TryGetValue("unique", out var unique);
        data.TryGetValue("tenant", out var tenant);
        data.TryGetValue("retries", out var retries);

#pragma warning disable CS4014

        var newTaskId = unique == true ? id : Guid.NewGuid().ToString("D");

        // when unique and task data exists -> abort transaction
        if (unique == true)
            tra.AddCondition(Condition.KeyNotExists(Prefix($"task:{newTaskId}")));

        tra.HashSetAsync(Prefix($"task:{newTaskId}"), new[]
        {
            new HashEntry("schedule", id),
            new HashEntry("tenant", tenant),
            new HashEntry("data", payload),
            new HashEntry("queue", queue),
            new HashEntry("retries", retries)
        }, CommandFlags.FireAndForget);

        // enqueue
        tra.ListLeftPushAsync(Prefix($"queue:{queue}"), newTaskId);

        return new ScheduleInfo
        {
            Cron = cronEx!,
            Timezone = (string?)tzv ?? "Etc/UTC",
            Unique = unique == true
        };
    }

    // Not Cluster safe / schedules + schedule:on same shard?
    public async Task<int> ExecuteSchedulesAsync()
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

                    var tra = db.CreateTransaction();

                    var info = await TriggerScheduleInternal(id, db, tra);

                    if (info == null)
                        continue; // TODO: ???

                    // when recurring
                    if (!string.IsNullOrWhiteSpace(info.Cron))
                    {
                        var cron = CronExpression.Parse(info.Cron);
                        var tz = TimeZoneInfo.FindSystemTimeZoneById(info.Timezone);
                        //var n = DateTimeOffset.FromUnixTimeSeconds((long)nextOffset);

                        // TODO: ???
                        var n = DateTimeOffset.UtcNow;

                        var next = cron.GetNextOccurrence(n, tz);


#pragma warning disable CS4014
                        if (next.HasValue)
                        {
                            tra.HashSetAsync(Prefix($"schedule:{id}"), "next", next.Value.ToUnixTimeSeconds(),
                                flags: CommandFlags.FireAndForget);
                            tra.SortedSetAddAsync(Prefix("schedules"), id, next.Value.ToUnixTimeSeconds(),
                                CommandFlags.FireAndForget);
                        }
                        else
                        {
                            tra.SortedSetRemoveAsync(Prefix("schedules"), id, CommandFlags.FireAndForget);
                            tra.KeyDeleteAsync(Prefix($"schedule:{id}"), CommandFlags.FireAndForget);
                        }
#pragma warning restore CS4014
                    }
                    else
                    {
#pragma warning disable CS4014
                        tra.SortedSetRemoveAsync(Prefix("schedules"), id, CommandFlags.FireAndForget);
                        tra.KeyDeleteAsync(Prefix($"schedule:{id}"), CommandFlags.FireAndForget);
#pragma warning restore CS4014
                    }

                    var executed = await tra.ExecuteAsync().ConfigureAwait(false);

                    if (info.Unique && !executed)
                        // transaction aborted -> update schedule
                        if (!string.IsNullOrWhiteSpace(info.Cron))
                        {
                            var cron = CronExpression.Parse(info.Cron);
                            var tz = TimeZoneInfo.FindSystemTimeZoneById(info.Timezone);

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

    // Cluster safe
    public async Task<BatchInfo?> GetBatchAsync(string batchId)
    {
        var db = _redis.GetDatabase();

        var res = await db.HashGetAllAsync(Prefix($"batch:{batchId}")).ConfigureAwait(false);

        if (res.Length == 0)
            return null;

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
            Remaining = (long)batch["remaining"]!,
            Scope = batch["scope"]!
        };
    }

    // Cluster safe
    public async Task<ICollection<BatchInfo>> GetBatchesAsync(string tenant, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var list = new List<BatchInfo>();
        var sids = db.SortedSetRangeByScore(Prefix($"batches:{tenant}"), skip: skip, take: take,
            flags: CommandFlags.PreferReplica);

        var res = new List<Task<HashEntry[]>>();

        foreach (var sid in sids)
            res.Add(db.HashGetAllAsync(Prefix($"batch:{sid}"), CommandFlags.PreferReplica));

        await Task.WhenAll(res).ConfigureAwait(false);

        foreach (var batchResult in res)
        {
            var batch = batchResult.Result.ToDictionary();
            var end = (long)batch["end"];
            list.Add(new BatchInfo
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
                Remaining = (long)batch["remaining"]!,
                Scope = batch["scope"]!
            });
        }

        return list;
    }

    public async Task<long> GetBatchesCountAsync(string tenant)
    {
        var db = _redis.GetDatabase();
        return await db.SortedSetLengthAsync(Prefix($"batches:{tenant}")).ConfigureAwait(false);
    }

    // Cluster safe
    public async Task<ICollection<QueueInfo>> GetQueuesAsync()
    {
        var db = _redis.GetDatabase();
        var list = new List<QueueInfo>();

        foreach (var q in _queues)
            list.Add(new QueueInfo
            {
                Name = UnPrefix(q!).Replace("queue:", string.Empty),
                Length = await db.ListLengthAsync(q),
                Checkout = await db.ListLengthAsync($"{q}:checkout"),
                Deadletter = _options.Deadletter ? await db.ListLengthAsync($"{q}:deadletter") : 0
            });

        return list;
    }

    // Cluster safe
    public async Task<QueueInfo> GetQueueAsync(string name)
    {
        var db = _redis.GetDatabase();

        return new QueueInfo
        {
            Name = name,
            Length = await db.ListLengthAsync(Prefix($"queue:{name}")),
            Checkout = await db.ListLengthAsync(Prefix($"queue:{name}:checkout")),
            Deadletter = await db.ListLengthAsync(Prefix($"queue:{name}:deadletter"))
        };
    }

    // Cluster safe
    public async Task<ICollection<TaskInfo>> GetTasksInQueueAsync(string queue, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var taskIds = await db.ListRangeAsync(Prefix(queue), skip, skip + take,
            CommandFlags.PreferReplica).ConfigureAwait(false);

        var list = new List<TaskInfo>(taskIds.Length);
        foreach (var taskId in taskIds)
        {
            var hashValues = await db.HashGetAllAsync(Prefix($"task:{taskId}"), CommandFlags.PreferReplica)
                .ConfigureAwait(false);
            var taskData = hashValues.ToDictionary();

            list.Add(new TaskInfo
            {
                Topic = taskData["topic"]!,
                TaskId = taskId!,
                Tenant = taskData["tenant"]!,
                Retries = (int)taskData["retries"]!,
                Data = taskData["data"]!
            });
        }

        return list;
    }

    // Cluster safe
    public async Task<ICollection<ScheduleInfo>> GetSchedulesAsync(string tenant, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var list = new List<ScheduleInfo>();
        var sids = await db.SortedSetRangeByScoreAsync(Prefix($"schedules:{tenant}"), skip: skip, take: take,
            flags: CommandFlags.PreferReplica).ConfigureAwait(false);

        var res = new List<Task<HashEntry[]>>();

        foreach (var sid in sids)
            res.Add(db.HashGetAllAsync(Prefix($"schedule:{sid}"), CommandFlags.PreferReplica));

        await Task.WhenAll(res).ConfigureAwait(false);

        foreach (var scheduleResult in res)
        {
            var schedule = scheduleResult.Result.ToDictionary();
            list.Add(new ScheduleInfo
            {
                Scope = schedule["scope"]!,
                Cron = schedule["cron"]!,
                Timezone = schedule["timezone"]!,
                Next = DateTimeOffset.FromUnixTimeSeconds((long)schedule["next"]).DateTime,
                Unique = (bool)schedule["unique"]
            });
        }

        return list;
    }

    public async Task<long> GetSchedulesCountAsync(string tenant)
    {
        var db = _redis.GetDatabase();
        return await db.SortedSetLengthAsync(Prefix($"schedules:{tenant}")).ConfigureAwait(false);
    }

    #endregion
}