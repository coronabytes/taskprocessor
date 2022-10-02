namespace Core.TaskProcessor;

public class QueueInfo
{
    public string Name { get; set; } = string.Empty;
    public long Length { get; set; }
    public long Checkout { get; set; }
    public long Deadletter { get; set; }
}