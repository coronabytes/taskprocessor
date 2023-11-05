namespace Core.TaskProcessor;

public class ScheduleData
{
    /// <summary>
    ///   unique identifier of schedule
    /// </summary>
    public string Id { get; set; } = string.Empty;
    
    /// <summary>
    ///  tenant identifier
    /// </summary>
    public string Tenant { get; set; } = string.Empty;

    /// <summary>
    ///   description
    /// </summary>
    public string Scope { get; set; } = string.Empty;

    /// <summary>
    ///  cron expressions (multiple cycles can be denoted with "|")
    /// </summary>
    public string Cron { get; set; } = string.Empty;

    /// <summary>
    ///   timezone e.g. Europe/Berlin
    /// </summary>
    public string? Timezone { get; set; }

    /// <summary>
    ///   if task has not been scheduled within this timespan, skip and continue with next occurrence after now
    /// </summary>
    public TimeSpan? Expire { get; set; }

    /// <summary>
    ///   if scheduled task from previous cycle has not completed yet, skip and continue with next occurrence after now
    /// </summary>
    public bool Unique { get; set; }

    /// <summary>
    ///   name of queue to schedule underlying task on
    /// </summary>
    public string Queue { get; set; } = string.Empty;
}