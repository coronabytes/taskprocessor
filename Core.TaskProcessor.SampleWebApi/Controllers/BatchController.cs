using Core.TaskProcessor.SampleWebApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace Core.TaskProcessor.SampleWebApi.Controllers;

[ApiController]
[Route("api/batches")]
public class BatchController : ControllerBase
{
    private readonly ITaskProcessor _processor;
    private readonly ISomeScopedService _someScopedService;

    public BatchController(ITaskProcessor processor, ISomeScopedService someScopedService)
    {
        _processor = processor;
        _someScopedService = someScopedService;
    }

    [HttpPost("enqueue")]
    public async Task<string> Enqueue()
    {
        await _processor.EnqueueBatchAsync("default", "core", new List<TaskData>
        {
            new(),
            new()
        }, new List<TaskData>
        {
            new()
            {
                Topic = "continue"
            }
        }, "some tasks");

        return "ok";
    }

    [HttpPost("expression")]
    public async Task<string> Expression()
    {
        return await _processor.EnqueueBatchAsync("default", "core", 
                () => _someScopedService.DoSomethingAsync("hello", CancellationToken.None),
                    () => _someScopedService.DoSomethingAsync("world", CancellationToken.None))
            .ConfigureAwait(false);
    }

    [HttpGet("list")]
    public async Task<IEnumerable<BatchInfo>> Get()
    {
        return await _processor.GetBatchesAsync("core").ConfigureAwait(false);
    }
}