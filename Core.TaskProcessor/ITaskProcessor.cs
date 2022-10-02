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
    /// <param name="tasks">list of tasks</param>
    /// <param name="continuations">tasks that will be queued when batch is done</param>
    /// <param name="scope">describes batch</param>
    /// <returns>batch id</returns>
    Task<string> EnqueueBatchAsync(string queue, string tenant, List<TaskData> tasks,
        List<TaskData>? continuations = null, string? scope = null);


    /// <summary>
    ///     appends tasks to existing a batch. continuations will run again if batch previously completed.
    /// </summary>
    /// <returns>returns false when batch does not exist</returns>
    Task<bool> AppendBatchAsync(string queue, string tenant, string batchId, List<TaskData> tasks);

    /// <summary>
    ///     cancel batch
    /// </summary>
    Task<bool> CancelBatchAsync(string batchId);

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
    Task<ICollection<TaskInfo>> GetTasksInQueueAsync(string queue, long skip = 0, long take = 50);

    /// <summary>
    ///     create or update schedule
    /// </summary>
    Task UpsertScheduleAsync(ScheduleData schedule, TaskData task);

    /// <summary>
    ///   execute schedule without affecting next execution
    /// </summary>
    /// <returns>true when task queued</returns>
    Task<bool> TriggerScheduleAsync(string id);

    /// <summary>
    ///     deletes schedule by id
    /// </summary>
    Task<bool> CancelScheduleAsync(string globalUniqueId, string tenant);


    /// <summary>
    ///     prolong checkout time
    /// </summary>
    Task<bool> ExtendLockAsync(string queue, string taskId, TimeSpan span);
}