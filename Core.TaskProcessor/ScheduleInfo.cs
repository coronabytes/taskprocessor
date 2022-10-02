namespace Core.TaskProcessor;

public class ScheduleInfo
{
    public string Id { get; set; } = string.Empty;
    public string Timezone { get; set; } = string.Empty;
    public string Scope { get; set; } = string.Empty;
    public string Cron { get; set; } = string.Empty;
    public DateTime? Next { get; set; }
    public bool Unique { get; set; }
}