using System.Collections.Concurrent;
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
        _queues = options.Queues.Select(x => (RedisKey)Prefix(x)).ToArray();

        _redis = ConnectionMultiplexer.Connect(options.Redis);

        // new jobs available -> fetch
        _redis.GetSubscriber().Subscribe(Prefix("task:events"), (channel, value) =>
        {
            FetchAsync().ContinueWith(_ => {});
        });

        // batch canceled -> search running jobs and terminate
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

        // global run state changed -> pause/resume jobs + schedules
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
            new HashEntry("scope", scope),
            new HashEntry("total", tasks.Count),
            new HashEntry("remaining", tasks.Count),
            new HashEntry("continuations",
                string.Join(' ', continuations?.Select(x => x.TaskId) ?? Array.Empty<string>()))
        });

        foreach (var task in tasks)
            tra.HashSetAsync(Prefix($"task:{task.TaskId}"), new[]
            {
                new HashEntry("id", task.TaskId),
                new HashEntry("batch", batchId),
                new HashEntry("data", task.Data),
                new HashEntry("topic", task.Topic),
                new HashEntry("retries", _options.Retries)
            });

        if (continuations?.Any() == true)
            foreach (var ctask in continuations)
                tra.HashSetAsync(Prefix($"task:{ctask.TaskId}"), new[]
                {
                    new HashEntry("id", ctask.TaskId),
                    new HashEntry("batch", batchId),
                    new HashEntry("continuation", true),
                    new HashEntry("data", ctask.Data),
                    new HashEntry("topic", ctask.Topic),
                    new HashEntry("retries", _options.Retries)
                });


        tra.ListLeftPushAsync(Prefix(queue), tasks.Select(x => (RedisValue)x.TaskId).ToArray());
        tra.PublishAsync(Prefix("task:events"), queue);
#pragma warning restore CS4014
        await tra.ExecuteAsync();
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

for i, v in ipairs(KEYS) do
local r = redis.call('rpoplpush', v, v.."":checkout"");
if r then
  local t = redis.call('time')[1] + ARGV[1];
  redis.call('zadd', v.."":pushback"", t, r);
  local jobData = redis.call('hgetall', ""{Prefix("task:")}""..r);
  local batchId = redis.call('hget', ""{Prefix("task:")}""..r, ""batch"");
  local batchData = redis.call('hgetall', ""{Prefix("batch:")}""..batchId);
  return {{v, r, jobData, batchData}}; 
end;
end
", _queues, new RedisValue[] { (long)_options.Retention.TotalSeconds });

        if (res.IsNull)
            return false;

        var r = (RedisResult[])res;
        var q = (string)r[0]!;
        var j = (string)r[1]!;
        var jobData = r[2].ToDictionary();

        // TODO: wenn schedule dann ist das null?
        var batchData = r[3].ToDictionary();

        var isContinuation = jobData.ContainsKey("continuation");

        if ((string)batchData["state"]! == "go" || isContinuation)
        {
            var cts = new CancellationTokenSource();
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, _shutdown.Token);

            var info = new TaskContext
            {
                Processor = this,
                TaskId = j,
                BatchId = (string)jobData["batch"]!,
                Tenant = (string)batchData["tenant"]!,
                Queue = UnPrefix(q),
                Cancel = linkedCts,
                Topic = (string?)jobData["topic"] ?? string.Empty,
                Data = (byte[])jobData["data"]!,
                IsContinuation = isContinuation
            };

            _tasks.TryAdd(info.TaskId, info);
            _actionBlock.Post(info);
        }
        else
        {
            

            _actionBlock.Post(new TaskContext
            {
                Processor = this,
                TaskId = j,
                BatchId = (string)jobData["batch"]!,
                IsCancellation = true,
                IsContinuation = jobData.ContainsKey("continuation"),
            Queue = q
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

    #region Schedule

    public async Task UpsertScheduleAsync(string globalUniqueId, string tenant, string scope, string topic, byte[] data, string queue,
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
                        new HashEntry("retries", _options.Retries),
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
                        new HashEntry("retries", 0)
                    }, CommandFlags.FireAndForget);

                    // enqueue
                    tra.ListLeftPushAsync(Prefix(queue), newTaskId);
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

    public Task<bool> ExtendLockAsync(string queue, string jobId, TimeSpan span)
    {
        var db = _redis.GetDatabase();
        return db.SortedSetUpdateAsync(Prefix($"{queue}:pushback"), jobId,
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
            var tra = db.CreateTransaction();
#pragma warning disable CS4014
            tra.ListRemoveAsync(Prefix($"{task.Queue}:checkout"), task.TaskId);
            tra.SortedSetRemoveAsync(Prefix($"{task.Queue}:pushback"), task.TaskId);
            tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "canceled");
            var remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");
#pragma warning restore CS4014
            await tra.ExecuteAsync().ConfigureAwait(false);

            if (await remaining <= 0)
            {
                var continuations = (string?)await db.HashGetAsync(Prefix($"batch:{task.BatchId}"), 
                    "continuations").ConfigureAwait(false) ?? string.Empty;

                var tra2 = db.CreateTransaction();
#pragma warning disable CS4014
                tra2.AddCondition(Condition.HashEqual(Prefix($"batch:{task.BatchId}"), "remaining", 0));
                tra2.AddCondition(Condition.HashNotEqual(Prefix($"batch:{task.BatchId}"), "state", "done"));
                tra2.HashSetAsync(Prefix($"batch:{task.BatchId}"), new[]
                {
                    new HashEntry("end", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                    new HashEntry("state", "done")
                });

                foreach (var c in continuations.Split(' ',
                             StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                    tra2.ListLeftPushAsync(Prefix($"{task.Queue}"), c);
#pragma warning restore CS4014
                await tra2.ExecuteAsync().ConfigureAwait(false);
            }

            await _options.OnTaskEnd(task).ConfigureAwait(false);
        }
        else
        {
            var start = DateTimeOffset.UtcNow;

            try
            {
                var retries = await db.HashDecrementAsync(Prefix($"task:{task.TaskId}"), "retries").ConfigureAwait(false);

                if (retries <= 0)
                {
                    var tra = db.CreateTransaction();
#pragma warning disable CS4014
                    tra.ListRemoveAsync(Prefix($"{task.Queue}:checkout"), task.TaskId, flags: CommandFlags.FireAndForget);
                    tra.SortedSetRemoveAsync(Prefix($"{task.Queue}:pushback"), task.TaskId, CommandFlags.FireAndForget);

                    if (!task.IsContinuation)
                    {
                        tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "failed",
                            flags: CommandFlags.FireAndForget);
                        tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining",
                            flags: CommandFlags.FireAndForget);
                    }

                    tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);
#pragma warning restore CS4014
                    await tra.ExecuteAsync();
                }
                else
                {
                    await _options.OnTaskStart(task).ConfigureAwait(false);

                    await Execute(task).ConfigureAwait(false);

                    var tra = db.CreateTransaction();
#pragma warning disable CS4014
                    tra.ListRemoveAsync(Prefix($"{task.Queue}:checkout"), task.TaskId,
                        flags: CommandFlags.FireAndForget);

                    var remaining = Task.FromResult(1L);

                    if (!task.IsContinuation) 
                    {
                        tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "done",
                            flags: CommandFlags.FireAndForget);
                        remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining",
                            flags: CommandFlags.FireAndForget);
                    }

                    tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "duration",
                        (DateTimeOffset.UtcNow - start).TotalSeconds, CommandFlags.FireAndForget);
                    tra.SortedSetRemoveAsync(Prefix($"{task.Queue}:pushback"), task.TaskId);

                    tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);
#pragma warning restore CS4014
                    await tra.ExecuteAsync().ConfigureAwait(false);

                    if (await remaining <= 0 && !task.IsContinuation)
                    {
                        // TODO: gibts doch schon in jobdata
                        var continuations = (string?)await db.HashGetAsync(Prefix($"batch:{task.BatchId}"),
                            "continuations").ConfigureAwait(false) ?? string.Empty;

                        var tra2 = db.CreateTransaction();
#pragma warning disable CS4014
                        tra2.AddCondition(Condition.HashEqual(Prefix($"batch:{task.BatchId}"), "remaining", 0));
                        tra2.AddCondition(Condition.HashNotEqual(Prefix($"batch:{task.BatchId}"), "state", "done"));
                        tra2.HashSetAsync(Prefix($"batch:{task.BatchId}"), new[]
                        {
                            new HashEntry("end", DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                            new HashEntry("state", "done")
                        });
                        foreach (var c in continuations.Split(' ',
                                     StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                            tra2.ListLeftPushAsync(Prefix($"{task.Queue}"), c);
#pragma warning restore CS4014
                        await tra2.ExecuteAsync().ConfigureAwait(false);
                    }

                    await _options.OnTaskEnd(task).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                var tra = db.CreateTransaction();
#pragma warning disable CS4014
                tra.ListRemoveAsync(Prefix($"{task.Queue}:checkout"), task.TaskId, flags: CommandFlags.FireAndForget);
                tra.ListLeftPushAsync(Prefix(task.Queue), task.TaskId, flags: CommandFlags.FireAndForget);
                tra.SortedSetRemoveAsync(Prefix($"{task.Queue}:pushback"), task.TaskId, CommandFlags.FireAndForget);
                
                // thats ok even for continuations
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
                Name = UnPrefix(q!),
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
            Length = await db.ListLengthAsync(Prefix(name)),
            Checkout = await db.SortedSetLengthAsync(Prefix($"{name}:checkout"))
        };
    }

    public async Task<ICollection<TaskData>> GetTasksInQueueAsync(string queue, long skip = 0, long take = 50)
    {
        var db = _redis.GetDatabase();

        var jobIds = await db.ListRangeAsync(Prefix(queue), skip, skip + take,
            CommandFlags.PreferReplica).ConfigureAwait(false);

        var list = new List<TaskData>(jobIds.Length);
        foreach (var jobId in jobIds)
        {
            var hashValues = await db.HashGetAllAsync(Prefix($"task:{jobId}"), CommandFlags.PreferReplica)
                .ConfigureAwait(false);
            var jobData = hashValues.ToDictionary();

            list.Add(new TaskData
            {
                Topic = jobData["topic"],
                TaskId = jobId,
                Tenant = jobData["tenant"],
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
end;
", _queues);
    }
}