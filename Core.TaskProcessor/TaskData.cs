namespace Core.TaskProcessor;

public class TaskData
{
    //public string TaskId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public byte[] Data { get; set; } = Array.Empty<byte>();
    public string? Queue { get; set; } = null;
    public int? Retries { get; set; } = null;
    public DateTimeOffset? DelayUntil { get; set; } = null;
}