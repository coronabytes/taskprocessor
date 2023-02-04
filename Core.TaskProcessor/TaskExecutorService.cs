using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Core.TaskProcessor;

internal class TaskExecutorService : BackgroundService
{
    private readonly ITaskProcessor _processor;
    private readonly IServiceProvider _serviceProvider;

    public TaskExecutorService(ITaskProcessor processor, IServiceProvider serviceProvider)
    {
        _processor = processor;
        _serviceProvider = serviceProvider;

        processor.Execute = Execute;
    }

    private async Task Execute(TaskContext ctx)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();

        if (ctx.Topic == "internal:expression:v1")
            await _processor.Executor.InvokeAsync(ctx, scope.ServiceProvider.GetRequiredService)
                .ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _processor.RunAsync(stoppingToken);
    }
}