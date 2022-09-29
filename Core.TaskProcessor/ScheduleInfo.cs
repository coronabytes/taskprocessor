namespace Core.TaskProcessor;

public class ScheduleInfo
{
    public string Id { get; set; }
    public string Timezone { get; set; }
    public string Scope { get; set; }
    public string Cron { get; set; }
    public DateTime? Next { get; set; }
}