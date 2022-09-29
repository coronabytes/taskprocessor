namespace Core.TaskProcessor;

public class TaskProcessorOptions
{
    /// <summary>
    ///   Redis prefix for all keys and channels
    /// </summary>
    public string Prefix { get; set; } = string.Empty;

    /// <summary>
    ///   Queues to listen for, order determines priority
    /// </summary>
    public string[] Queues { get; set; } = { "default" };

    /// <summary>
    ///   Max concurrent work on all queues
    /// </summary>
    public int MaxWorkers { get; set; } = Environment.ProcessorCount;

    /// <summary>
    ///   How frequent to poll when no work available
    /// </summary>
    public TimeSpan PollFrequency { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(14);
    public int Retries { get; set; } = 3;

    /// <summary>
    ///   Deduplication window
    /// </summary>
    public TimeSpan Invisibility { get; set; } = TimeSpan.FromMinutes(5);

    public Func<TaskContext, Task> OnTaskStart { get; set; } = _ => Task.CompletedTask;
    public Func<TaskContext, Task> OnTaskEnd { get; set; } = _ => Task.CompletedTask;

    public string Redis { get; set; }
}