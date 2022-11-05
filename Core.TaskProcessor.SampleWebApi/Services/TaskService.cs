using System.Linq.Expressions;

namespace Core.TaskProcessor.SampleWebApi.Services;

public class TaskService : BackgroundService, ITaskService
{
    private readonly ITaskProcessor _processor;
    private readonly IServiceProvider _serviceProvider;

    public TaskService(ITaskProcessor processor, IServiceProvider serviceProvider)
    {
        _processor = processor;
        _serviceProvider = serviceProvider;

        processor.Execute = Execute;
    }

    public Task<string> EnqueueAsync(Expression<Func<Task>> methodCall, string queue)
    {
        var exec = _serviceProvider.GetRequiredService<IRemoteExpressionExecutor>();

        return _processor.EnqueueBatchAsync(queue, "core", new List<TaskData>
        {
            new()
            {
                Topic = "expression",
                Data = exec.Serialize(methodCall)
            }
        });
    }

    private async Task Execute(TaskContext ctx)
    {
        if (ctx.Topic == "expression")
        {
            await using var scope = _serviceProvider.CreateAsyncScope();
            var exec = _serviceProvider.GetRequiredService<IRemoteExpressionExecutor>();
            await exec.InvokeAsync(ctx, scope).ConfigureAwait(false);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _processor.ResumeAsync();
        await _processor.RunAsync(stoppingToken);
    }
}