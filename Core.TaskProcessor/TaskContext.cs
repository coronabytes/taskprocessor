namespace Core.TaskProcessor;

public class TaskContext : TaskData
{
    public string TaskId { get; set; } = string.Empty;
    public string Tenant { get; internal set; } = null!;
    public ITaskProcessor Processor { get; internal set; } = null!;
    public string? BatchId { get; set; }
    public string? ScheduleId { get; set; }
    internal CancellationTokenSource CancelSource { get; set; } = null!;
    public CancellationToken CancelToken { get; internal set; } = CancellationToken.None;
    public bool IsCancellation { get; set; }
    public bool IsContinuation { get; set; }

    //public BatchInfo Batch { get; set; } = null!;

    public Task<bool> ExtendLockAsync(TimeSpan duration)
    {
        return Processor.ExtendLockAsync(Queue!, TaskId, duration);
    }

    public void Cancel()
    {
        CancelSource.Cancel();
    }
}