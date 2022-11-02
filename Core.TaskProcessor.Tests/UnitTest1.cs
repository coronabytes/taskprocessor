using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Core.TaskProcessor.Tests;

public class UnitTest1
{
    private readonly ITestOutputHelper _output;
    private readonly TaskProcessor _processor;

    public UnitTest1(ITestOutputHelper output)
    {
        _output = output;
        _processor = new TaskProcessor(new TaskProcessorOptions
        {
            Prefix = "{dev}",
            MaxWorkers = 4,
            Queues = new[] { "q1", "q2", "q3" },
            Redis = "localhost:6379,abortConnect=false",
            Retries = 3,
            Deadletter = true,
            OnTaskStart = info =>
            {
                _output.WriteLine($"Start: {info.Queue} {info.Topic}");
                return Task.CompletedTask;
            },
            OnTaskEnd = info =>
            {
                _output.WriteLine($"End: {info.Queue} {info.Topic}");
                return Task.CompletedTask;
            }
        })
        {
            Execute = async info =>
            {
                //await info.ExtendLockAsync(TimeSpan.FromMinutes(5));
                _output.WriteLine($"Process: {info.Queue} {info.Topic}");
                //throw new Exception("error");
                await Task.Delay(500, info.Cancel.Token);
            }
        };
    }

    [Fact]
    public async Task Enqueue()
    {
        var batchId = await _processor.EnqueueBatchAsync("q2", "1001", new List<TaskData>
        {
            new()
            {
                Topic = "t1"
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

        var batches = await _processor.GetBatchesAsync("1001");
        _output.WriteLine(JsonConvert.SerializeObject(batches, Formatting.Indented));
    }

    [Fact]
    public async Task Schedule()
    {
        await _processor.UpsertScheduleAsync(new ScheduleData
        {
            ScheduleId = "123",
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
    public async Task Cleanup()
    {
        await _processor.CleanUpAsync();
    }
}