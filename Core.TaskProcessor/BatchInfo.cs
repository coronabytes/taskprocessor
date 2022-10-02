namespace Core.TaskProcessor;

public class BatchInfo
{
    public string Id { get; set; } = string.Empty;
    public string Scope { get; set; } = string.Empty;
    public DateTime Start { get; set; }
    public DateTime? End { get; set; }
    public long Done { get; set; }
    public long Canceled { get; set; }
    public long Failed { get; set; }
    public long Remaining { get; set; }
    public long Total { get; set; }
    public double Duration { get; set; }
    public string State { get; set; } = string.Empty;
}