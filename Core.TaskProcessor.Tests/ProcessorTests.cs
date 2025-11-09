using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Core.TaskProcessor.Tests;

public class ProcessorTests
{
    private readonly ITestOutputHelper _output;
    private readonly TaskProcessor _processor;

    public ProcessorTests(ITestOutputHelper output)
    {
        _output = output;
        _processor = new TaskProcessor(new TaskProcessorOptions
        {
            Prefix = "{dev}",
            MaxWorkers = 1,
            Queues = new[] { "q1", "fair_q2", "q3" },
            Redis = "localhost:6379,abortConnect=false",
            Retries = 3,
            Deadletter = true,
            OnTaskStart = info =>
            {
                //_output.WriteLine($"Start: {info.Queue} {info.Topic}");
                return Task.CompletedTask;
            },
            OnTaskEnd = info =>
            {
                //_output.WriteLine($"End: {info.Queue} {info.Topic}");
                return Task.CompletedTask;
            }
        })
        {
            Execute = async info =>
            {
                //await info.ExtendLockAsync(TimeSpan.FromMinutes(5));
                _output.WriteLine($"Process: {info.Queue} {info.Topic}");
                //throw new Exception("error");
                await Task.Delay(10, info.CancelToken);
            }
        };
    }

    [Fact]
    public async Task EnqueueFairness()
    {
        for (int i = 0; i < 10; i++)
        {
            await _processor.EnqueueTaskAsync("fair_q2", "1001", new TaskData
            {
                Topic = "A",
            });
        }

        await Task.Delay(500);

        for (int i = 0; i < 10; i++)
        {
            await _processor.EnqueueTaskAsync("fair_q2", "1002", new TaskData
            {
                Topic = "B",
            });
        }
    }

    [Fact]
    public async Task EnqueueNoFairness()
    {
        for (int i = 0; i < 10; i++)
        {
            await _processor.EnqueueTaskAsync("q1", "1001", new TaskData
            {
                Topic = "A",
            });
        }

        await Task.Delay(500);

        for (int i = 0; i < 10; i++)
        {
            await _processor.EnqueueTaskAsync("q1", "1002", new TaskData
            {
                Topic = "B",
            });
        }
    }

    [Fact]
    public async Task Enqueue()
    {
        var batchId = await _processor.EnqueueBatchAsync("q2", "1001", new List<TaskData>
        {
            new()
            {
                Topic = "t1",
                //DelayUntil = DateTimeOffset.UtcNow.AddMinutes(1)
            }
            //new(),
            //new()
        }, new List<TaskData>
        {
            new()
            {
                Topic = "c1",
                Queue = "q1"
            },
            new()
            {
                Topic = "c2",
                Queue = "q3"
            }
        });

        _output.WriteLine($"Batch: {batchId}");
    }


    [Fact]
    public async Task ListBatches()
    {
        var batches = await _processor.GetBatchesAsync("1001");
        _output.WriteLine(JsonConvert.SerializeObject(batches, Formatting.Indented));
    }

    [Fact]
    public async Task ListQueues()
    {
        var queues = await _processor.GetQueuesAsync();
        _output.WriteLine(JsonConvert.SerializeObject(queues, Formatting.Indented));
    }

    [Fact]
    public async Task Run()
    {
        await _processor.ResumeAsync();

        var t = _processor.RunAsync(CancellationToken.None);

        await Task.Delay(10000);

        await _processor.StopAsync();

        //var batches = await _processor.GetBatchesAsync("1001");
        //_output.WriteLine(JsonConvert.SerializeObject(batches, Formatting.Indented));
    }

    [Fact]
    public async Task Schedule()
    {
        await _processor.UpsertScheduleAsync(new ScheduleData
        {
            Id = "123",
            Tenant = "1001",
            Scope = "Fetch Emails",
            Cron = "*/2 * * * *",
            Timezone = "Etc/UTC",
            Unique = true
        }, new TaskData
        {
            Topic = "email",
            Queue = "q1",
            Data = new byte[] { },
            Retries = 3
        });
    }

    [Fact]
    public async Task TriggerSchedule()
    {
        await _processor.TriggerScheduleAsync("123");
    }

    [Fact]
    public async Task ExecuteSchedules()
    {
        var run = await _processor.ExecuteSchedulesAsync();

        _output.WriteLine($"Run: {run}");

        var schedules = await _processor.GetSchedulesAsync("1001");
        _output.WriteLine(JsonConvert.SerializeObject(schedules, Formatting.Indented));
    }

    [Fact]
    public async Task Pushback()
    {
        await _processor.PushbackAsync();
    }

    [Fact]
    public async Task Cleanup()
    {
        await _processor.CleanUpAsync();
    }

    [Fact]
    public async Task RetryTasks()
    {
        var tasks = await _processor.RetryDeadTasksAsync("q2", 3);

        _output.WriteLine($"{tasks} retried");
    }
}