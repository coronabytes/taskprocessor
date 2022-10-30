namespace Core.TaskProcessor.SampleWebApi;

public class TaskService : BackgroundService
{
    private readonly ITaskProcessor _processor;
    private readonly IServiceProvider _serviceProvider;

    public TaskService(ITaskProcessor processor, IServiceProvider serviceProvider)
    {
        _processor = processor;
        _serviceProvider = serviceProvider;

        processor.Execute = Execute;
    }

    private async Task Execute(TaskContext ctx)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();

        await Task.Delay(1000, ctx.Cancel.Token);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _processor.ResumeAsync();
        await _processor.RunAsync(stoppingToken);
    }
}