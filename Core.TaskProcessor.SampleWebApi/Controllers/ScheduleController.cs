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
    public async Task Create([FromQuery] string id = "my-unique-id",  
        [FromQuery] string cron = "10 * * * * * | */30 * * * * *",
        [FromQuery] string timezone = "Europe/Berlin"
        )
    {
        await _processor.ResumeAsync().ConfigureAwait(false);

        await _processor.UpsertScheduleAsync(new ScheduleData
            {
                Id = id,
                Cron = cron,
                Timezone = timezone,
                Tenant = "core",
                Unique = true,
                Expire = TimeSpan.FromSeconds(30),
                Queue = "default"
            }, () => _someScopedService.DoSomethingAsync($"{id} {timezone}", CancellationToken.None))
            .ConfigureAwait(false);
    }

    [HttpPost("cancel")]
    public async Task Cancel([FromQuery] string id = "my-unique-id")
    {
        await _processor.CancelScheduleAsync(id, "core").ConfigureAwait(false);
    }

    [HttpGet("list")]
    public async Task<IEnumerable<ScheduleInfo>> Get()
    {
        return await _processor.GetSchedulesAsync("core", 0, 100).ConfigureAwait(false);
    }
}