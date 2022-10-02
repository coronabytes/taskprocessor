namespace Core.TaskProcessor;

public class TaskContext : TaskData
{
    public string TaskId { get; set; } = string.Empty;
    public string Tenant { get; internal set; } = null!;
    public ITaskProcessor Processor { get; internal set; } = null!;
    public string? BatchId { get; set; }
    public string? ScheduleId { get; set; }
    public CancellationTokenSource Cancel { get; set; } = null!;
    public bool IsCancellation { get; set; }
    public bool IsContinuation { get; set; }

    //public BatchInfo Batch { get; set; } = null!;

    public Task<bool> ExtendLockAsync(TimeSpan duration)
    {
        return Processor.ExtendLockAsync(Queue!, TaskId, duration);
    }
}