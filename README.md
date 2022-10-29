[![Nuget](https://img.shields.io/nuget/v/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)
[![Nuget](https://img.shields.io/nuget/dt/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)

```
dotnet add package Core.TaskProcessor
```

# .NET Background Task Processing Engine
*Hangfire Pro Redis* except:
- way less features
- open source (Apache 2.0)
- redis 6+ storage engine (cluster mode with hash slot per prefix)
- low level api (bring your own serialization / ui)
- multi tenancy
- global pause and resume of all processing
- every task belongs to a batch
- batch cancelation can abort in process tasks "instantly"
- easy to access batch statistics per tenant
- async extension points with access to batch information

## Initialization

```csharp
var proc = new TaskProcessor(new TaskProcessorOptions
{
    Prefix = "{dev}", // redis cluster mode needs single hash slot
    MaxWorkers = 4,
    Queues = new[] { "high", "low" }, // pop queues from left to right - first non empty queue wins
    Redis = "localhost:6379,abortConnect=false",
    Retries = 3,
    Retention = TimeSpan.FromDays(14), // batch information will be kept this long
    Invisibility = TimeSpan.FromMinutes(5), // task will be redelivered when taking longer than this
    OnTaskStart = info =>
    {
        return Task.CompletedTask;
    },
    OnTaskEnd = info =>
    {
        return Task.CompletedTask;
    }
})
{
    Execute = async info =>
    {
        // TODO: Do your background work here
        await Task.Delay(500, info.Cancel.Token);
    }
};
```

## Enqueue batch tasks

```csharp
var batchId = await proc.EnqueueBatchAsync("low", "my-tenant",  new List<TaskData>
{
    new()
    {
        Topic = "send-email",
        Data = Encoding.UTF8.GetBytes("Reasonably small payload")
    },
    new()
    {
        Topic = "send-email"
    },
}, new List<TaskData>
{
    new()
    {
        Topic = "all-emails-send",
        Queue = "high"
    }
});
```

## Cancel batch
```csharp
await proc.CancelBatchAsync(batchId);
```

## Schedule tasks

```csharp
await proc.UpsertScheduleAsync(new ScheduleData
{
    ScheduleId = "unique-schedule-id",
    Tenant = "my-tenant",
    Scope = "Send hourly email",
    Cron = "0 */1 * * *",
    Timezone = "Etc/UTC",
    Unique = true // if task hasn't completed yet - do not schedule again
}, new TaskData
{
    Topic = "send-email",
    Data = Encoding.UTF8.GetBytes("Reasonably small payload")
    Queue = "low",
    Retries = 3
});
```

## Cancel schedule
```csharp
await proc.CancelScheduleAsync("unique-schedule-id", "my-tenant");
```

## Service Worker / AspNetCore

```csharp

builder.Services.AddSingleton<ITaskProcessor>(new TaskProcessor(new TaskProcessorOptions
{
  // ...
}));
builder.Services.AddHostedService<TaskService>();

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
        // TODO: Do something on scope
        await Task.Delay(1000, ctx.Cancel.Token);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _processor.RunAsync(stoppingToken);
    }
}
```



