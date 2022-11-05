namespace Core.TaskProcessor;

public class TaskProcessorOptions
{
    /// <summary>
    ///     redis prefix for all keys and channels
    ///     cluster mode requires hash notation e.g. {taskproc-1}
    /// </summary>
    public string Prefix { get; set; } = string.Empty;

    /// <summary>
    ///     queues to listen for, order determines priority
    /// </summary>
    public string[] Queues { get; set; } = { "default" };

    /// <summary>
    ///     max concurrent work on all queues for this instance
    /// </summary>
    public int MaxWorkers { get; set; } = Environment.ProcessorCount;

    /// <summary>
    ///     how frequent to poll when no work available
    ///     also schedule / cleanup
    /// </summary>
    public TimeSpan PollFrequency { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     how long batches are kept
    /// </summary>
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(14);

    /// <summary>
    ///     if tasks failed this many times they will be discarded or deadlettered
    /// </summary>
    public int Retries { get; set; } = 3;

    /// <summary>
    ///     when retries are exhausted move to deadletter list instead of discard
    /// </summary>
    public bool Deadletter { get; set; } = true;

    /// <summary>
    ///     deduplication window
    /// </summary>
    public TimeSpan Invisibility { get; set; } = TimeSpan.FromMinutes(5);

    public Func<TaskContext, Task> OnTaskStart { get; set; } = _ => Task.CompletedTask;
    public Func<TaskContext, Task> OnTaskEnd { get; set; } = _ => Task.CompletedTask;

    public string Redis { get; set; } = string.Empty;

    public bool UseHostedService { get; set; }

    public IRemoteExpressionExecutor ExpressionExecutor { get; set; } = new RemoteExpressionExecutor();
}