namespace Core.TaskProcessor;

public interface ITaskProcessor
{
    bool IsPaused { get; }

    /// <summary>
    ///     task execution handler
    /// </summary>
    Func<TaskContext, Task> Execute { get; set; }

    /// <summary>
    ///     globally pauses all task and schedule processing
    /// </summary>
    Task Pause();

    /// <summary>
    ///     globally resumes all task and schedule processing
    /// </summary>
    Task Resume();

    /// <summary>
    ///     enqueue a batch of tasks with optional continuation tasks
    /// </summary>
    /// <param name="queue">queue to run tasks and continuations on</param>
    /// <param name="tenant">tenant id</param>
    /// <param name="batchId">unique batch id</param>
    /// <param name="tasks">list of tasks</param>
    /// <param name="continuations">tasks that will be queued when batch is done</param>
    /// <param name="scope">describes batch</param>
    Task EnqueueBatchAsync(string queue, string tenant, string batchId, List<TaskData> tasks,
        List<TaskData>? continuations = null, string? scope = null);


    /// <summary>
    ///     appends tasks to existing a batch. continuations will run again if batch previously completed.
    /// </summary>
    /// <returns>returns false when batch does not exist</returns>
    Task<bool> AppendBatchAsync(string queue, string tenant, string batchId, List<TaskData> tasks);

    /// <summary>
    ///     cancel batch
    /// </summary>
    Task CancelBatchAsync(string batchId);

    /// <summary>
    ///     manually ask for work (generally not needed to call this)
    /// </summary>
    Task<bool> FetchAsync();

    /// <summary>
    ///     stop main loop
    /// </summary>
    Task StopAsync();

    /// <summary>
    ///     main loop
    /// </summary>
    Task RunAsync(CancellationToken cancellationToken);

    /// <summary>
    ///     list batches for tenant with paging support
    /// </summary>
    Task<ICollection<BatchInfo>> GetBatchesAsync(string tenant, long skip = 0, long take = 50);

    /// <summary>
    ///     list schedules for tenant with paging support
    /// </summary>
    Task<ICollection<ScheduleInfo>> GetSchedules(string tenant, long skip = 0, long take = 50);

    /// <summary>
    ///     List tasks in queue with paging support
    /// </summary>
    Task<ICollection<TaskData>> GetTasksInQueueAsync(string queue, long skip = 0, long take = 50);

    /// <summary>
    ///     create or update schedule
    /// </summary>
    /// <param name="globalUniqueId">guid</param>
    /// <param name="tenant">tenant</param>
    /// <param name="scope">scope</param>
    /// <param name="topic">topic</param>
    /// <param name="data">payload</param>
    /// <param name="queue">queue the task will be scheduled on</param>
    /// <param name="cron">cron expression (* * * * *)</param>
    /// <param name="timeZoneId">IANA Timezone ID (Etc/UTC)</param>
    /// <param name="unique">Stops scheduling another task when previous task not done</param>
    Task UpsertScheduleAsync(string globalUniqueId, string tenant, string scope, string topic, byte[] data,
        string queue, string cron, string? timeZoneId = null, bool unique = false);

    /// <summary>
    ///     deletes schedule by id
    /// </summary>
    Task<bool> CancelScheduleAsync(string globalUniqueId, string tenant);


    /// <summary>
    ///     prolong checkout time
    /// </summary>
    Task<bool> ExtendLockAsync(string queue, string taskId, TimeSpan span);
}