namespace Core.TaskProcessor;

public class ScheduleData
{
    public string ScheduleId { get; set; } = string.Empty;
    public string Tenant { get; set; } = string.Empty;
    public string Scope { get; set; } = string.Empty;
    public string Cron { get; set; } = string.Empty;
    public string? Timezone { get; set; }
    public bool Unique { get; set; }
}