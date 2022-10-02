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
                _output.WriteLine($"End: {info.Queue} {info.BatchId} {info.TaskId}");
                return Task.CompletedTask;
            },
            OnTaskEnd = info =>
            {
                _output.WriteLine($"End: {info.Queue} {info.BatchId} {info.TaskId}");
                return Task.CompletedTask;
            }
        })
        {
            Execute = async info =>
            {
                //await info.ExtendLockAsync(TimeSpan.FromMinutes(5));
                _output.WriteLine($"Process: {info.Queue} {info.BatchId} {info.TaskId}");
                throw new Exception("error");
                await Task.Delay(500, info.Cancel.Token);
            }
        };
    }

    [Fact]
    public async Task Enqueue()
    {
        var batchId = Guid.NewGuid().ToString("D");

        await _processor.EnqueueBatchAsync("q2", "1001", batchId, new List<TaskData>
        {
            new()
            {
                TaskId = Guid.NewGuid().ToString("D")
            },
            new()
            {
                TaskId = Guid.NewGuid().ToString("D")
            },
            new()
            {
                TaskId = Guid.NewGuid().ToString("D")
            }
        }, new List<TaskData>
        {
            new()
            {
                Topic = "c1",
                TaskId = Guid.NewGuid().ToString("D"),
                Queue = "q1"
            },
            new()
            {
                Topic = "c2",
                TaskId = Guid.NewGuid().ToString("D"),
                Queue = "q3"
            }
        });

        var batches = await _processor.GetBatchesAsync("1001");
        _output.WriteLine(JsonConvert.SerializeObject(batches, Formatting.Indented));

        var queues = await _processor.GetQueuesAsync();
        _output.WriteLine(JsonConvert.SerializeObject(queues, Formatting.Indented));
    }


    [Fact]
    public async Task ListBatches()
    {
        var batches = await _processor.GetBatchesAsync("1001");
        _output.WriteLine(JsonConvert.SerializeObject(batches, Formatting.Indented));
    }

    [Fact]
    public async Task Run()
    {
        await _processor.Resume();

        var t = _processor.RunAsync(CancellationToken.None);

        await Task.Delay(10000);

        await _processor.StopAsync();

        var batches = await _processor.GetBatchesAsync("1001");
        _output.WriteLine(JsonConvert.SerializeObject(batches, Formatting.Indented));
    }

    [Fact]
    public async Task Schedule()
    {
        await _processor.UpsertScheduleAsync("123", "1001", "Fetch Emails", "email", new byte[] { }, "q1",
            "*/2 * * * *", "Europe/Berlin");
    }

    [Fact]
    public async Task ExecuteSchedules()
    {
        var run = await _processor.ExecuteSchedules();

        _output.WriteLine($"Run: {run}");

        var schedules = await _processor.GetSchedules("1001");
        _output.WriteLine(JsonConvert.SerializeObject(schedules, Formatting.Indented));
    }

    [Fact]
    public async Task Cleanup()
    {
        await _processor.CleanUp();
    }
}