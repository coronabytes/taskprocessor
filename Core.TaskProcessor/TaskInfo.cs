namespace Core.TaskProcessor;

public class TaskInfo : TaskData
{
    public string TaskId { get; set; } = string.Empty;
    public string Tenant { get; set; } = string.Empty;
}