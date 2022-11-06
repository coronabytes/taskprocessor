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

    [HttpPost("create")]
    public async Task<string> Create()
    {
        await _processor.ResumeAsync();

        return await _processor.EnqueueBatchAsync("default", "core", batch =>
            {
                batch.Enqueue(() => _someScopedService.DoSomethingAsync("hello", CancellationToken.None));
                batch.Enqueue(() => _someScopedService.DoSomethingAsync("world", CancellationToken.None));
                
                batch.ContinueWith(() => _someScopedService.DoSomething("!"), "high");
            })
            .ConfigureAwait(false);
    }

    [HttpGet("list")]
    public async Task<IEnumerable<BatchInfo>> Get()
    {
        return await _processor.GetBatchesAsync("core").ConfigureAwait(false);
    }
}