[![Nuget](https://img.shields.io/nuget/v/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)
[![Nuget](https://img.shields.io/nuget/dt/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)

```
dotnet add package Core.TaskProcessor
```

# .NET Background Task Processing Engine
*Hangfire Pro Redis* except:
- open source (Apache 2.0)
- exclusive redis/elasticache 6+ storage engine (cluster mode supported)
- multi tenancy
- global pause and resume of all processing
- every task belongs to a batch
- batch cancelation can abort in process tasks "instantly"
- easy to access batch statistics per tenant
- async extension points with access to batch information

## Initialization in AspNetCore

```csharp
builder.Services.AddTaskProcessor(new TaskProcessorOptions
{
    Redis = "localhost:6379,abortConnect=false",
    Prefix = "{coretask}", // redis cluster mode needs single hash slot
    Queues = new[] { "high", "default" }, // pop queues from left to right - first non empty queue wins
    MaxWorkers = 4, // action block concurrency limit
    Retries = 3,
    Invisibility = TimeSpan.FromMinutes(5), // task will be redelivered when taking longer than this
    PollFrequency = TimeSpan.FromSeconds(10), // schedule + cleanup frequency
    Retention = TimeSpan.FromDays(7), // batch information will be kept this long
    UseHostedService = true // use supplied background worker service
});
```

## Enqueue batch tasks

```csharp
var batchId = await _processor.EnqueueBatchAsync("default", "my-tenant", batch =>
{
    batch.Enqueue(() => _someScopedService.DoSomethingAsync("hello", CancellationToken.None));
    batch.Enqueue(() => _someScopedService.DoSomethingAsync("world", CancellationToken.None));

    batch.ContinueWith(() => _someScopedService.DoSomething("!"), "high");
})
```

## What functions can be invoked?
- static functions
- functions on types resolveable by the IServiceProvider (scoped interfaces preferred)
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
    ScheduleId = "unique-schedule-id",
    Tenant = "my-tenant",
    Scope = "Send hourly email",
    Cron = "0 */1 * * *",
    Timezone = "Etc/UTC",
    Unique = true // if task hasn't completed yet - do not schedule again
}, () => _someScopedService.DoSomething("!"), "high");
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



