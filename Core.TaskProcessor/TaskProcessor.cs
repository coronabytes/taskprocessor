using Cronos;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Core.TaskProcessor;

public class TaskProcessor : ITaskProcessor
{
    private readonly ActionBlock<TaskContext> _actionBlock;
    private readonly TaskProcessorOptions _options;
    private readonly RedisKey[] _queueKeys;
    private readonly string[] _queuesNames;
    private readonly ConnectionMultiplexer _redis;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, TaskContext> _tasks = new();
    private DateTime _nextCleanup = DateTime.UtcNow;
    private DateTime _nextPushback = DateTime.UtcNow;

    public TaskProcessor(TaskProcessorOptions options)
    {
        _options = options;
        _queueKeys = options.Queues.Select(x => (RedisKey)Prefix($"queue:{x}")).ToArray();
        _queuesNames = options.Queues.ToArray();

        _redis = ConnectionMultiplexer.Connect(options.Redis);

        _actionBlock = new ActionBlock<TaskContext>(Process, new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = _options.MaxWorkers
        });
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

            if (!task.DelayUntil.HasValue)
            {
                if (!push.ContainsKey(q))
                    push.Add(q, new List<RedisValue>());

                push[q].Add(taskId);
            }

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

            if (task.DelayUntil.HasValue)
                tra.SortedSetAddAsync(Prefix($"queue:{queue}:pushback"), taskId,
                    task.DelayUntil.Value.ToUnixTimeSeconds());
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
            tra.SortedSetAddAsync(Prefix("queues"), q.Key, DateTimeOffset.UtcNow.ToUnixTimeSeconds());

            if (q.Key.Contains("[fair]"))
            {
                tra.HashIncrementAsync(Prefix($"queue:{q.Key}:fairness"), tenant, q.Value.Count);
                tra.ListLeftPushAsync(Prefix($"queue:{q.Key}:{tenant}"), q.Value.ToArray());
            }
            else
                tra.ListLeftPushAsync(Prefix($"queue:{q.Key}"), q.Value.ToArray());


            tra.PublishAsync(RedisChannel.Literal(Prefix($"queue:{q.Key}:event")), "fetch");
        }

#pragma warning restore CS4014
        await tra.ExecuteAsync().ConfigureAwait(false);

        return batchId;
    }

    public async Task<string> EnqueueTaskAsync(string queue, string tenant, TaskData task)
    {
        var taskId = Guid.NewGuid().ToString("D");

        var q = task.Queue ?? queue;

        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        
        tra.HashSetAsync(Prefix($"task:{taskId}"), [
            new HashEntry("id", taskId),
            new HashEntry("tenant", tenant),
            new HashEntry("data", task.Data),
            new HashEntry("topic", task.Topic),
            new HashEntry("queue", q),
            new HashEntry("retries", task.Retries ?? _options.Retries)
        ]);

        if (task.DelayUntil.HasValue)
            tra.SortedSetAddAsync(Prefix($"queue:{queue}:pushback"), taskId,
                task.DelayUntil.Value.ToUnixTimeSeconds());
        else
        {
            tra.SortedSetAddAsync(Prefix("queues"), q, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            
            if (queue.Contains("[fair]"))
            {
                tra.HashIncrementAsync(Prefix($"queue:{q}:fairness"), tenant);
                tra.ListLeftPushAsync(Prefix($"queue:{q}:{tenant}"), taskId);
            }
            else
                tra.ListLeftPushAsync(Prefix($"queue:{q}"), taskId);

            tra.PublishAsync(RedisChannel.Literal(Prefix($"queue:{q}:event")), "fetch");
        }

#pragma warning enable CS4014
            await tra.ExecuteAsync().ConfigureAwait(false);
        return taskId;
    }

    public Task<string> EnqueueTaskAsync(string queue, string tenant, Expression<Func<Task>> methodCall, DateTimeOffset? delayUntil = null, int? retries = null)
    {
        return EnqueueTaskAsync(queue, tenant, new TaskData
        {
            Topic = "internal:expression:v1",
            Data = _options.ExpressionExecutor.Serialize(methodCall),
            DelayUntil = delayUntil,
            Queue = queue,
            Retries = retries
        });
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
            tra.SortedSetAddAsync(Prefix("queues"), q.Key, DateTimeOffset.UtcNow.ToUnixTimeSeconds());

            if (queue.Contains("[fair]"))
            {
                tra.HashIncrementAsync(Prefix($"queue:{q}:fairness"), tenant, q.Value.Count);
                tra.ListLeftPushAsync(Prefix($"queue:{q.Key}:{tenant}"), q.Value.ToArray());
            }
            else
                tra.ListLeftPushAsync(Prefix($"queue:{q.Key}"), q.Value.ToArray());


            tra.PublishAsync(RedisChannel.Literal(Prefix($"queue:{q.Key}:event")), "fetch");
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
        tra.PublishAsync(RedisChannel.Literal(Prefix("global:cancel")), batchId, CommandFlags.FireAndForget);
#pragma warning restore CS4014
        await tra.ExecuteAsync();

        return await ok;
    }

    public async Task<bool> FetchAsync()
    {
        if (IsPaused)
            return false;

        try
        {
            var db = _redis.GetDatabase();

            var res = await db.ScriptEvaluateAsync($@"
for i, queue in ipairs(KEYS) do

if string.find(queue, '[fair]') then
  local tenant = redis.call('hrandfield', queue.."":fairness"");

  if tenant then
    local taskId = redis.call('rpoplpush', queue.."":""..tenant, queue.."":checkout"");

    if taskId then
      local ctr = redis.call('hincrby', queue.."":fairness"", tenant, -1);

      if ctr <= 0 then
        redis.call('hdel', queue.."":fairness"", tenant);
      end;

      local invis = redis.call('time')[1] + ARGV[1];
      redis.call('zadd', queue.."":pushback"", invis, taskId);
      local taskData = redis.call('hgetall', ""{Prefix("task:")}""..taskId);
      local batchId = redis.call('hget', ""{Prefix("task:")}""..taskId, ""batch"");

      if batchId then
        local batchData = redis.call('hgetall', ""{Prefix("batch:")}""..batchId);
        return {{queue, taskId, taskData, batchData}}; 
      end;

      return {{queue, taskId, taskData}}; 
    end;
  end;
else
  local taskId = redis.call('rpoplpush', queue, queue.."":checkout"");
  if taskId then
    local invis = redis.call('time')[1] + ARGV[1];
    redis.call('zadd', queue.."":pushback"", invis, taskId);
    local taskData = redis.call('hgetall', ""{Prefix("task:")}""..taskId);
    local batchId = redis.call('hget', ""{Prefix("task:")}""..taskId, ""batch"");

    if batchId then
      local batchData = redis.call('hgetall', ""{Prefix("batch:")}""..batchId);
      return {{queue, taskId, taskData, batchData}}; 
    end;

    return {{queue, taskId, taskData}}; 
  end;
end;

end
", _queueKeys, new RedisValue[] { (long)_options.Retention.TotalSeconds });

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
                        CancelSource = linkedCts,
                        CancelToken = linkedCts.Token,
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
                    CancelSource = linkedCts,
                    CancelToken = linkedCts.Token,
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
            else // single task
            {
                var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);

                var info = new TaskContext
                {
                    Processor = this,
                    TaskId = j,
                    Tenant = (string)taskData["tenant"]!,
                    Queue = (string?)taskData["queue"],
                    CancelSource = linkedCts,
                    CancelToken = linkedCts.Token,
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
        catch (Exception)
        {
            return false;
        }
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

        var db = _redis.GetDatabase();

        IsPaused = !(bool)await db.StringGetAsync(Prefix("global:run"));

        _redis.GetSubscriber().Subscribe(RedisChannel.Literal(Prefix("global:cancel")), (channel, value) =>
        {
            var batchId = (string)value!;

            foreach (var task in _tasks.Values)
                try
                {
                    if (task.BatchId == batchId && !task.IsCancellation)
                        task.Cancel();
                }
                catch (Exception)
                {
                    //
                }
        });

        _redis.GetSubscriber().Subscribe(RedisChannel.Literal(Prefix("global:run")), (channel, value) => { IsPaused = !(bool)value; });

        foreach (var q in _options.Queues)
            _redis.GetSubscriber().Subscribe(RedisChannel.Literal(Prefix($"queue:{q}:event")),
                (channel, value) => { FetchAsync().ContinueWith(_ => { }); });

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        foreach (var q in _queuesNames)
            await db.SortedSetAddAsync(Prefix("queues"), q, now);

        while (!cancel.IsCancellationRequested)
        {
            if (IsPaused)
            {
                await Task.Delay(_options.BaseFrequency, cancel).ContinueWith(_ => { });
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
            await PushbackAsync();
            await CleanUpAsync();

            await Task.Delay(_options.BaseFrequency, cancel).ContinueWith(_ => { });
        }
    }

    public Task<bool> ExtendLockAsync(string queue, string taskId, TimeSpan span)
    {
        var db = _redis.GetDatabase();
        return db.SortedSetUpdateAsync(Prefix($"queue:{queue}:pushback"), taskId,
            DateTimeOffset.UtcNow.Add(span).ToUnixTimeSeconds());
    }

    public async Task PushbackAsync()
    {
        if (_nextPushback > DateTime.UtcNow)
            return;
        _nextPushback = DateTime.UtcNow.Add(_options.PushbackFrequency);

        var db = _redis.GetDatabase();

        var lockTaken = await db.LockTakeAsync(Prefix("pushback-lock"), Environment.MachineName,
            _options.PushbackFrequency).ConfigureAwait(false);

        if (!lockTaken)
            return;

        await db.ScriptEvaluateAsync($@"
local t = redis.call('time')[1];

local queues = redis.call('zrange', KEYS[1], 0, t+60, 'BYSCORE', 'LIMIT', 0, 1000);

for i, v in ipairs(queues) do
  local q = ""{Prefix("queue:")}""..v
  local r = redis.call('zrange', q.."":pushback"", 0, t, 'BYSCORE', 'LIMIT', 0, 500);
  for j, w in ipairs(r) do
    redis.call('zadd', ""{Prefix("queues")}"", t, v);
    
    if string.find(q, '[fair]') then
      local tenant = redis.call('hget', q..':'..w, 'tenant');
      redis.call('hincrby', q..':fairness', tenant);
      redis.call('lpush', q..':'..tenant, taskId);
    else  
      redis.call('lpush', q, w);
    end

    redis.call('zrem', q.."":pushback"", w);
    redis.call('lrem', q.."":checkout"", 0, w);
  end;
end;
", new RedisKey[] { Prefix("queues") });
    }

    public async Task CleanUpAsync()
    {
        if (_nextCleanup > DateTime.UtcNow)
            return;
        _nextCleanup = DateTime.UtcNow.Add(_options.CleanUpFrequency);

        var db = _redis.GetDatabase();

        var lockTaken = await db.LockTakeAsync(Prefix("cleanup-lock"), Environment.MachineName,
            _options.CleanUpFrequency).ConfigureAwait(false);

        if (!lockTaken)
            return;

        await db.ScriptEvaluateAsync($@"
local t = redis.call('time')[1];
local batches = redis.call('zrange', ""{Prefix("batches:cleanup")}"", 0, t, 'BYSCORE', 'LIMIT', 0, 500);

for k, batchId in ipairs(batches) do
  local tenant = redis.call('hget', ""{Prefix("batch:")}""..batchId, ""tenant"");
  redis.call('zrem', ""{Prefix("batches:cleanup")}"", batchId);
  redis.call('zrem', ""{Prefix("batches:")}""..tenant, batchId);
  redis.call('del', ""{Prefix("batch:")}""..batchId);
  redis.call('del', ""{Prefix("batch:")}""..batchId.."":continuations"");
end;
", new RedisKey[] { Prefix("batches:cleanup") });
    }

    public async Task<long> RetryDeadTasksAsync(string queue, int? retries = null, long? count = null)
    {
        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync($@"
local t = redis.call('time')[1];
local taskIds = redis.call('zrange', KEYS[2], 0, t, 'BYSCORE', 'LIMIT', 0, ARGV[1]);

for j, taskId in ipairs(taskIds) do
  local batchId = redis.call('hget', ""{Prefix("task:")}""..taskId, ""batch"");
  redis.call('hset', '{Prefix("task:")}'..taskId, 'retries', ARGV[2]);
  redis.call('hincrby', '{Prefix("batch:")}'..batchId, 'failed', -1)
  redis.call('hset', '{Prefix("batch:")}'..batchId, 'state', 'go')

  if string.find(KEYS[1], '[fair]') then
    local tenant = redis.call('hget', ""{Prefix("task:")}""..taskId, 'tenant');
    redis.call('hincrby', KEYS[1]..':fairness', tenant);
    redis.call('lpush', KEYS[1]..':'..tenant, taskId);
  else  
    redis.call('lpush', KEYS[1], taskId);
  end
  
  redis.call('zrem', KEYS[2], taskId);
  redis.call('publish', KEYS[1]..':event', 'fetch');
end;

return #(taskIds);
", new RedisKey[]
        {
            Prefix($"queue:{queue}"),
            Prefix($"queue:{queue}:deadletter")
        }, new RedisValue[]
        {
            count ?? 100, retries ?? _options.Retries
        }).ConfigureAwait(false);

        return (long)res;
    }

    public async Task<long> DiscardDeadTasksAsync(string queue, long? count = null)
    {
        var db = _redis.GetDatabase();

        var res = await db.ScriptEvaluateAsync($@"
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

    private string Prefix(string s)
    {
        if (!string.IsNullOrWhiteSpace(_options.Prefix))
            return $"{_options.Prefix}:{s}";
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
                if (!string.IsNullOrEmpty(task.BatchId))
                {
                    tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "canceled",
                        flags: CommandFlags.FireAndForget);
                    remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");
                }

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

            long retries = 0;

            try
            {
                retries = await db.HashDecrementAsync(Prefix($"task:{task.TaskId}"), "retries")
                    .ConfigureAwait(false);

                if (retries < 0)
                {
                    //var remaining = Task.FromResult(1L);

                    var tra = db.CreateTransaction();
#pragma warning disable CS4014
                    tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId,
                        flags: CommandFlags.FireAndForget);
                    tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId,
                        CommandFlags.FireAndForget);

                    if (!task.IsContinuation)
                    {
                        if (!string.IsNullOrEmpty(task.BatchId))
                            tra.HashIncrementAsync(Prefix($"batch:{task.BatchId}"), "failed",
                                flags: CommandFlags.FireAndForget);
                        //remaining = tra.HashDecrementAsync(Prefix($"batch:{task.BatchId}"), "remaining");
                        if (_options.Deadletter && (string.IsNullOrEmpty(task.ScheduleId) || _options.DeadletterSchedules))
                            tra.SortedSetAddAsync(Prefix($"queue:{task.Queue}:deadletter"), task.TaskId,
                                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                        else
                            tra.KeyDeleteAsync(Prefix($"task:{task.TaskId}"), CommandFlags.FireAndForget);
                    }
#pragma warning restore CS4014
                    await tra.ExecuteAsync().ConfigureAwait(false);

                    // failures can no longer complete batch
                    //if (await remaining <= 0 && !task.IsContinuation)
                    //    await CompleteBatchAsync(task, db);
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
                        if (!string.IsNullOrEmpty(task.BatchId))
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
            catch (Exception ex)
            {
                try
                {
                    await _options.OnTaskError(task, ex).ConfigureAwait(false);
                }
                catch
                {
                    //
                }

                TimeSpan? delay = null;

                try
                {
                    delay = await _options.OnTaskFailedDelay(task, retries);
                }
                catch
                {
                    //
                }

                var tra = db.CreateTransaction();
#pragma warning disable CS4014
                tra.ListRemoveAsync(Prefix($"queue:{task.Queue}:checkout"), task.TaskId,
                    flags: CommandFlags.FireAndForget);

                if (delay.HasValue)
                {
                    tra.SortedSetUpdateAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId,
                        DateTimeOffset.UtcNow.Add(delay.Value).ToUnixTimeSeconds(), flags: CommandFlags.FireAndForget);
                }
                else
                {
                    if (task.Queue!.Contains("[fair]"))
                    {
                        tra.HashIncrementAsync(Prefix($"queue:{task.Queue}:fairness"), task.Tenant);
                        tra.ListLeftPushAsync(Prefix($"queue:{task.Queue}:{task.Tenant}"), task.TaskId,
                            flags: CommandFlags.FireAndForget);
                    }
                    else
                        tra.ListLeftPushAsync(Prefix($"queue:{task.Queue}"), task.TaskId,
                            flags: CommandFlags.FireAndForget);

                    tra.SortedSetRemoveAsync(Prefix($"queue:{task.Queue}:pushback"), task.TaskId,
                        CommandFlags.FireAndForget);
                }

                if (!task.IsContinuation && !string.IsNullOrEmpty(task.BatchId))
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
        if (string.IsNullOrEmpty(task.BatchId))
            return;

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
local t = redis.call('time')[1];
local continuations = redis.call('lrange', KEYS[1], 0, 100);

for i, taskId in ipairs(continuations) do
  local q = redis.call('hget', '{Prefix("task:")}'..taskId, 'queue');

  redis.call('zadd', ""{Prefix("queues")}"", t, q);

  if string.find(queue, '[fair]') then
     local tenant = redis.call('hget', '{Prefix("task:")}'..taskId, 'tenant');
     redis.call('hincrby', '{Prefix("queue:")}'..q..':fairness', tenant);
     redis.call('lpush', '{Prefix("queue:")}'..q..':'..tenant, taskId);
  else  
    redis.call('lpush', '{Prefix("queue:")}'..q, taskId);
  end

  redis.call('publish', '{Prefix("queue:")}'..q..':event', 'fetch');
end;

", new RedisKey[] { Prefix($"batch:{task.BatchId}:continuations") });
#pragma warning restore CS4014
        await tra2.ExecuteAsync().ConfigureAwait(false);
    }

    #region Control

    public bool IsPaused { get; private set; } = true;

    public async Task PauseAsync()
    {
        await _redis.GetDatabase().StringSetAsync(Prefix("global:run"), 0);
        await _redis.GetSubscriber().PublishAsync(RedisChannel.Literal(Prefix("global:run")), false);
    }

    public async Task ResumeAsync()
    {
        await _redis.GetDatabase().StringSetAsync(Prefix("global:run"), 1);
        await _redis.GetSubscriber().PublishAsync(RedisChannel.Literal(Prefix("global:run")), true);
    }

    #endregion

    #region Schedule

    private DateTimeOffset? NextCronOccurrence(string cron, string? timezone)
    {
        var tz = TimeZoneInfo.FindSystemTimeZoneById(timezone ?? "Etc/UTC");
        var now = DateTime.UtcNow;

        var splits = cron.Split("|", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var next = splits.Select(x=> (DateTimeOffset?)CronExpression.Parse(x, _options.UseCronSeconds ? CronFormat.IncludeSeconds : CronFormat.Standard).GetNextOccurrence(now, tz))
            .Where(x=>x.HasValue)
            .MinBy(x=>x!.Value);

        return next;
    }

    public async Task UpsertScheduleAsync(ScheduleData schedule, TaskData task)
    {
        if (task.Queue == null)
            throw new ArgumentNullException(nameof(task.Queue));

        var next = NextCronOccurrence(schedule.Cron, schedule.Timezone);

        var queue = task.Queue ?? schedule.Queue;

        var db = _redis.GetDatabase();
        var tra = db.CreateTransaction();
#pragma warning disable CS4014
        tra.HashSetAsync(Prefix($"schedule:{schedule.Id}"), new[]
        {
            new HashEntry("id", schedule.Id),
            new HashEntry("timezone", schedule.Timezone ?? "Etc/UTC"),
            new HashEntry("data", task.Data),
            new HashEntry("topic", task.Topic),
            new HashEntry("queue", queue),
            new HashEntry("scope", schedule.Scope),
            new HashEntry("tenant", schedule.Tenant),
            new HashEntry("retries", task.Retries ?? _options.Retries),
            new HashEntry("cron", schedule.Cron),
            new HashEntry("next", next!.Value.ToUnixTimeSeconds()),
            new HashEntry("unique", schedule.Unique),
            new HashEntry("expire", (long)(schedule.Expire?.TotalSeconds ?? -1d)),
        }).ConfigureAwait(false);
        tra.SortedSetAddAsync(Prefix("schedules"), schedule.Id, next.Value.ToUnixTimeSeconds());
        tra.SortedSetAddAsync(Prefix($"schedules:{schedule.Tenant}"), schedule.Id, 0);
        tra.SortedSetAddAsync(Prefix("queues"), queue, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
#pragma warning restore CS4014
        await tra.ExecuteAsync().ConfigureAwait(false);
    }

    public Task UpsertScheduleAsync(ScheduleData schedule, Expression<Func<Task>> methodCall)
    {
        return UpsertScheduleAsync(schedule, new TaskData
        {
            Topic = "internal:expression:v1",
            Data = _options.ExpressionExecutor.Serialize(methodCall),
            Queue = schedule.Queue
        });
    }

    public Task UpsertScheduleAsync(ScheduleData schedule, Expression<Action> methodCall)
    {
        return UpsertScheduleAsync(schedule, new TaskData
        {
            Topic = "internal:expression:v1",
            Data = _options.ExpressionExecutor.Serialize(methodCall),
            Queue = schedule.Queue
        });
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

        var info = await TriggerScheduleInternal(id, db, tra, DateTimeOffset.UtcNow).ConfigureAwait(false);

        if (info == null)
            return false;

        return await tra.ExecuteAsync().ConfigureAwait(false);
    }

    // not cluster safe - queue and task on same shard???
    private async Task<ScheduleInfo?> TriggerScheduleInternal(string id, IDatabase db, ITransaction tra,
        DateTimeOffset scheduledTime)
    {
        var data = (await db.HashGetAllAsync(Prefix($"schedule:{id}"))).ToDictionary(x => x.Name,
            x => x.Value);

        if (data.Count == 0)
            return null;

        data.TryGetValue("cron", out var cronEx);
        data.TryGetValue("timezone", out var tzv);
        //data.TryGetValue("next", out var nextOffset);
        data.TryGetValue("queue", out var queue);
        data.TryGetValue("data", out var payload);
        data.TryGetValue("topic", out var topic);
        data.TryGetValue("unique", out var unique);
        data.TryGetValue("tenant", out var tenant);
        data.TryGetValue("retries", out var retries);
        data.TryGetValue("expire", out var expire);

        // if expired do nothing
        if ((long)expire > 0)
        {
            var delta = (long)(DateTimeOffset.UtcNow - scheduledTime).TotalSeconds;
            var clamped = Math.Max(0L, delta);

            if (clamped > (long)expire)
                return new ScheduleInfo
                {
                    Cron = cronEx!,
                    Timezone = (string?)tzv ?? "Etc/UTC",
                    Unique = unique == true,
                    Expire = TimeSpan.FromSeconds((long)expire)
                };
        }

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
            new HashEntry("topic", topic),
            new HashEntry("queue", queue),
            new HashEntry("retries", retries)
        }, CommandFlags.FireAndForget);

        // enqueue

        if (((string)queue).Contains("[fair]"))
        {
            tra.HashIncrementAsync(Prefix($"queue:{queue}:fairness"), tenant);
            tra.ListLeftPushAsync(Prefix($"queue:{queue}:{tenant}"), newTaskId, flags: CommandFlags.FireAndForget);
        }
        else
            tra.ListLeftPushAsync(Prefix($"queue:{queue}"), newTaskId, flags: CommandFlags.FireAndForget);

        tra.SortedSetAddAsync(Prefix("queues"), queue, DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            CommandFlags.FireAndForget);
        tra.PublishAsync(RedisChannel.Literal(Prefix($"queue:{queue}:event")), "fetch", CommandFlags.FireAndForget);
#pragma warning restore CS4014

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

            var tasks = await db.SortedSetRangeByScoreWithScoresAsync(Prefix("schedules"), 0, now, take: 100).ConfigureAwait(false);
            //var tasks = await db.SortedSetRangeByScoreAsync(Prefix("schedules"), 0, now, take: 100)
            //    .ConfigureAwait(false);

            foreach (var entry in tasks)
                try
                {
                    var time = DateTimeOffset.FromUnixTimeSeconds((long)entry.Score);
                    var id = (string)entry.Element!;

                    var tra = db.CreateTransaction();

                    var info = await TriggerScheduleInternal(id, db, tra, time).ConfigureAwait(false);

                    if (info == null)
                        continue;

                    // when recurring
                    if (!string.IsNullOrWhiteSpace(info.Cron))
                    {
                        var next = NextCronOccurrence(info.Cron, info.Timezone);

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
                            var next = NextCronOccurrence(info.Cron, info.Timezone);

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

    public async Task<ICollection<QueueInfo>> GetQueuesAsync()
    {
        var db = _redis.GetDatabase();
        var list = new List<QueueInfo>();

        foreach (var q in await db.SortedSetRangeByScoreAsync(Prefix("queues")).ConfigureAwait(false))
            list.Add(new QueueInfo
            {
                Name = (string?)q ?? string.Empty,
                Length = await db.ListLengthAsync(Prefix($"queue:{q}")),
                Checkout = await db.ListLengthAsync(Prefix($"queue:{q}:checkout")),
                Pushback = await db.SortedSetLengthAsync(Prefix($"queue:{q}:pushback")),
                Deadletter = await db.SortedSetLengthAsync(Prefix($"queue:{q}:deadletter"))
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
            // TODO: global tracking of fair queue length
            Length =  name.Contains("[fair]") ? -1 : await db.ListLengthAsync(Prefix($"queue:{name}")),
            Checkout = await db.ListLengthAsync(Prefix($"queue:{name}:checkout")),
            Pushback = await db.SortedSetLengthAsync(Prefix($"queue:{name}:pushback")),
            Deadletter = await db.SortedSetLengthAsync(Prefix($"queue:{name}:deadletter"))
        };
    }

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
                Id = schedule["id"]!,
                Scope = schedule["scope"]!,
                Cron = schedule["cron"]!,
                Timezone = schedule["timezone"]!,
                Next = DateTimeOffset.FromUnixTimeSeconds((long)schedule["next"]).DateTime,
                Expire = (long)schedule["expire"] < 0 ? TimeSpan.FromSeconds((long)schedule["expire"]) : null,
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

    #region Expression

    private class BatchBuilder : IBatchContinue
    {
        private readonly TaskProcessor _taskProcessor;

        public BatchBuilder(TaskProcessor taskProcessor)
        {
            _taskProcessor = taskProcessor;
        }

        public List<TaskData> Tasks { get; } = new();
        public List<TaskData> Continuations { get; } = new();

        public void Enqueue(Expression<Func<Task>> methodCall, string? queue = null, DateTimeOffset? delayUntil = null)
        {
            Tasks.Add(new TaskData
            {
                Topic = "internal:expression:v1",
                Data = _taskProcessor.Executor.Serialize(methodCall),
                Queue = queue,
                DelayUntil = delayUntil
            });
        }

        public void ContinueWith(Expression<Func<Task>> methodCall, string? queue = null)
        {
            Continuations.Add(new TaskData
            {
                Topic = "internal:expression:v1",
                Data = _taskProcessor.Executor.Serialize(methodCall),
                Queue = queue
            });
        }

        public void Enqueue(Expression<Action> methodCall, string? queue = null, DateTimeOffset? delayUntil = null)
        {
            Tasks.Add(new TaskData
            {
                Topic = "internal:expression:v1",
                Data = _taskProcessor.Executor.Serialize(methodCall),
                Queue = queue,
                DelayUntil = delayUntil
            });
        }

        public void ContinueWith(Expression<Action> methodCall, string? queue = null)
        {
            Continuations.Add(new TaskData
            {
                Topic = "internal:expression:v1",
                Data = _taskProcessor.Executor.Serialize(methodCall),
                Queue = queue
            });
        }
    }

    public Task<string> EnqueueBatchAsync(string queue, string tenant, Action<IBatchContinue> batchAction)
    {
        var batch = new BatchBuilder(this);
        batchAction(batch);
        return EnqueueBatchAsync(queue, tenant, batch.Tasks, batch.Continuations);
    }

    public Task<bool> AppendBatchAsync(string queue, string tenant, string batchId, Action<IBatchEnqueue> batchAction)
    {
        var batch = new BatchBuilder(this);
        batchAction(batch);
        return AppendBatchAsync(queue, tenant, batchId, batch.Tasks);
    }

    public IRemoteExpressionExecutor Executor => _options.ExpressionExecutor;

    #endregion
}