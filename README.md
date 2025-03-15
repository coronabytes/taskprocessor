[![Nuget](https://img.shields.io/nuget/v/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)
[![Nuget](https://img.shields.io/nuget/dt/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)

```
dotnet add package Core.TaskProcessor
```

# .NET Background Task Processing Engine
*Hangfire Pro Redis* except:
- open source (Apache 2.0)
- exclusive redis/valkey/elasticache 6.2+ storage engine (cluster mode supported)
- multi tenancy
- global pause and resume of all processing
- batch cancelation can abort in process tasks "instantly"
- easy to access batch statistics per tenant
- async extension points with access to batch information

## Initialization in AspNetCore

```csharp
builder.Services.AddTaskProcessor((sp, options) =>
{
    options.Redis = "localhost:6379,abortConnect=false";
    options.Prefix = "{coretask}"; // redis cluster mode needs single hash slot
    options.Queues = ["high", "default", "low"]; // pop queues from left to right - first non empty queue wins
    options.MaxWorkers = 4; // action block concurrency limit
    options.Retries = 3; // if tasks fails x times its discarded or deadlettered
    options.Invisibility = TimeSpan.FromMinutes(5); // task will be redelivered when taking longer than this
    options.BaseFrequency = TimeSpan.FromSeconds(5); // fetches tasks when reactive events failed
    options.PushbackFrequency = TimeSpan.FromSeconds(10); // how often to run task retry/delay pushbacks
    options.CleanUpFrequency = TimeSpan.FromMinutes(5); // how often to run batch cleanups
    options.Retention = TimeSpan.FromDays(7); // batch information will be kept this long
    options.Deadletter = true; // move failed tasks to deadletter queues
    options.DeadletterUniqueSchedules = false; // ignore deadletter for unique schedules or they will pause indefinatly
    options.UseCronSeconds = false;
    options.OnTaskFailedDelay = (_, retry) => // delay retry on task failure
        Task.FromResult(retry switch
        {
            2 => TimeSpan.FromSeconds(5),
            1 => TimeSpan.FromSeconds(60),
            _ => (TimeSpan?)null
        });
    options.OnTaskError = (_, exception) =>
    {
        sp.GetRequiredService<ILogger<ITaskProcessor>>()
            .LogError(exception, "Task Error");

        return Task.CompletedTask;
    };
});
builder.Services.AddTaskProcessorExecutor();
```

## Enqueue batch tasks

```csharp
var batchId = await _processor.EnqueueBatchAsync("default", "my-tenant", batch =>
{
    batch.Enqueue(() => _someScopedService.DoSomethingAsync("hello", CancellationToken.None));
    
    batch.Enqueue(() => _someScopedService.DoSomethingAsync("world", CancellationToken.None), 
      delayUntil: DateTimeOffset.UtcNow.AddSeconds(30));

    batch.ContinueWith(() => _someScopedService.DoSomething("!"), "high");
})
```

## What functions can be invoked?
- static functions
- functions on types resolveable by the IServiceProvider (scoped interfaces preferred)
- return type of void or Task
- all parameters need to be json serializable with the default implementation (custom implementations possible)
- constant parameters are fastest, yet complex lists and objects are also possible (dynamic invoked)
- only the first method call will run in background - all parameters will be evaluated at enqueue time
- beware of functions in strongly named assemblys 

## Append tasks to batch (warning: continuations will run only once)

```csharp
await _processor.AppendBatchAsync("default", "my-tenant", batchId, batch =>
{
    batch.Enqueue(() => _someScopedService.DoSomethingAsync("hello 2", CancellationToken.None));
    batch.Enqueue(() => _someScopedService.DoSomethingAsync("world 2", CancellationToken.None));
})
```

## Cancel batch
```csharp
await _processor.CancelBatchAsync(batchId);
```

## Schedule tasks

```csharp
await _processor.UpsertScheduleAsync(new ScheduleData
{
    Id = "unique-schedule-id",
    Tenant = "my-tenant",
    Queue = "default",
    Scope = "Send hourly email",
    Cron = "0 */1 * * *",
    Timezone = "Etc/UTC",
    Unique = true // if task is enqueued from previous cycle and hasn't completed yet, this cycle will be skipped
}, () => _someScopedService.DoSomethingAsync("scheduled task", CancellationToken.None));
```

## Cancel schedule
```csharp
await proc.CancelScheduleAsync("unique-schedule-id", "my-tenant");
```

## Get runtime infos
```csharp
await proc.GetBatchesAsync("my-tenant", 0, 25);
await proc.GetTasksInQueueAsync("low", 0, 25);
```

## custom background worker
If you need to integrate another ioc solution set UseHostedService = false
and provide a custom one. Executor.InvokeAsync has an Type to instance resolver callback.
```csharp
class CustomExecutorService : BackgroundService
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
            await _processor.Executor.InvokeAsync(ctx,
                    type => scope.ServiceProvider.GetRequiredService(type))
                .ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _processor.RunAsync(stoppingToken);
    }
}
```



