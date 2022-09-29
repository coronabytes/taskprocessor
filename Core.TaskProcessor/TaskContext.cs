namespace Core.TaskProcessor;

public class TaskContext : TaskData
{
    public ITaskProcessor Processor { get; internal set; }
    public string Queue { get; set; } = string.Empty;
    public string BatchId { get; set; }
    public CancellationTokenSource Cancel { get; set; }
    public bool IsCancellation { get; set; }
    public bool IsContinuation { get; set; }

    public Task<bool> ExtendLockAsync(TimeSpan duration)
    {
        return Processor.ExtendLockAsync(Queue, TaskId, duration);
    }

    public BatchInfo Batch { get; set; }
}