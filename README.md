[![Nuget](https://img.shields.io/nuget/v/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)
[![Nuget](https://img.shields.io/nuget/dt/Core.TaskProcessor)](https://www.nuget.org/packages/Core.TaskProcessor)

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
_processor = new TaskProcessor(new TaskProcessorOptions
{
    Prefix = "{dev}",
    MaxWorkers = 4,
    Queues = new[] { "high", "low" },
    Redis = "localhost:6379,abortConnect=false",
    Retries = 3,
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
