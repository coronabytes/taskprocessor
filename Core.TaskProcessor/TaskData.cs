namespace Core.TaskProcessor;



public class TaskData
{
    public string Tenant { get; set; } = string.Empty;
    public string TaskId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public byte[] Data { get; set; } = Array.Empty<byte>();
    //public bool ContinueOnError { get; set; }
    //public bool ContinueOnSuccess { get; set; }
}