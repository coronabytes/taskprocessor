namespace Core.TaskProcessor;

public interface ITaskProcessor
{
    bool IsPaused { get; }

    /// <summary>
    ///   Task Execution Handler
    /// </summary>
    Func<TaskContext, Task> Execute { get; set; }

    /// <summary>
    ///  Globally pauses all task and schedule processing
    /// </summary>
    Task Pause();

    /// <summary>
    ///  Globally resumes all task and schedule processing
    /// </summary>
    Task Resume();
    Task EnqueueBatchAsync(string queue, string tenant, string batchId, List<TaskData> jobs, List<TaskData>? continuations = null, string? scope = null);
    Task CancelBatchAsync(string batchId);

    /// <summary>
    ///  Manually ask for work (generally not needed to call this)
    /// </summary>
    Task<bool> FetchAsync();

    /// <summary>
    ///   Stop Main Loop
    /// </summary>
    Task StopAsync();

    /// <summary>
    ///   Main Loop
    /// </summary>
    Task RunAsync(CancellationToken cancellationToken);

    /// <summary>
    ///   List batches for tenant with paging support
    /// </summary>
    Task<ICollection<BatchInfo>> GetBatchesAsync(string tenant, long skip = 0, long take = 50);

    /// <summary>
    ///   List schedules for tenant with paging support
    /// </summary>
    Task<ICollection<ScheduleInfo>> GetSchedules(string tenant, long skip = 0, long take = 50);

    /// <summary>
    ///   List tasks in queue with paging support
    /// </summary>
    Task<ICollection<TaskData>> GetTasksInQueueAsync(string queue, long skip = 0, long take = 50);

    /// <summary>
    ///   Create or update schedule
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
    Task UpsertScheduleAsync(string globalUniqueId, string tenant, string scope, string topic, byte[] data, string queue, string cron, string? timeZoneId = null, bool unique = false);
    
    /// <summary>
    ///   Deletes schedule by id
    /// </summary>
    Task<bool> CancelScheduleAsync(string globalUniqueId, string tenant);


    /// <summary>
    ///  Prolong Checkout time
    /// </summary>
    Task<bool> ExtendLockAsync(string queue, string taskId, TimeSpan span);
}