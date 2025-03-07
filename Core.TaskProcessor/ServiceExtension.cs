using Microsoft.Extensions.DependencyInjection;

namespace Core.TaskProcessor;

public static class ServiceExtension
{
    public static IServiceCollection AddTaskProcessor(this IServiceCollection collection, TaskProcessorOptions options)
    {
        collection.AddSingleton<ITaskProcessor>(new TaskProcessor(options));

        return collection;
    }

    public static IServiceCollection AddTaskProcessor(this IServiceCollection collection, Action<IServiceProvider, TaskProcessorOptions> configure)
    {
        var options = new TaskProcessorOptions();

        collection.AddSingleton<ITaskProcessor>(sp =>
        {
            configure(sp, options);

            return new TaskProcessor(options);
        });

        return collection;
    }

    public static IServiceCollection AddTaskProcessorExecutor(this IServiceCollection collection)
    {
        collection.AddHostedService<TaskExecutorService>();

        return collection;
    }
}