using Microsoft.Extensions.DependencyInjection;

namespace Core.TaskProcessor;

public static class ServiceExtension
{
    public static IServiceCollection AddTaskProcessor(this IServiceCollection collection, TaskProcessorOptions options)
    {
        //collection.AddSingleton<IRemoteExpressionExecutor, RemoteExpressionExecutor>();
        collection.AddSingleton<ITaskProcessor>(new TaskProcessor(options));

        if (options.UseHostedService)
            collection.AddHostedService<TaskExecutorService>();

        return collection;
    }
}