using Core.TaskProcessor.SampleWebApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace Core.TaskProcessor.SampleWebApi.Controllers;

[ApiController]
[Route("api/schedules")]
public class ScheduleController : ControllerBase
{
    private readonly ITaskProcessor _processor;
    private readonly ISomeScopedService _someScopedService;

    public ScheduleController(ITaskProcessor processor, ISomeScopedService someScopedService)
    {
        _processor = processor;
        _someScopedService = someScopedService;
    }

    [HttpPost("create")]
    public async Task Create()
    {
        await _processor.ResumeAsync().ConfigureAwait(false);

        await _processor.UpsertScheduleAsync(new ScheduleData
            {
                Id = "my-unique-id",
                Cron = "*/1 * * * *",
                Tenant = "core",
                Unique = true,
                Queue = "default"
            }, () => _someScopedService.DoSomethingAsync("scheduled task!", CancellationToken.None))
            .ConfigureAwait(false);
    }

    [HttpPost("cancel")]
    public async Task Cancel()
    {
        await _processor.CancelScheduleAsync("my-unique-id", "core").ConfigureAwait(false);
    }

    [HttpGet("list")]
    public async Task<IEnumerable<ScheduleInfo>> Get()
    {
        return await _processor.GetSchedulesAsync("core").ConfigureAwait(false);
    }
}