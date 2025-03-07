using Core.TaskProcessor;
using Core.TaskProcessor.SampleWebApi.Services;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.AddTaskProcessor((sp, options) =>
{
    options.Redis = "localhost:6379,abortConnect=false";
    options.Prefix = "{core}";
    options.Queues = ["high", "default"];
    options.MaxWorkers = 4;
    options.Retries = 3;
    options.Invisibility = TimeSpan.FromMinutes(5);
    options.BaseFrequency = TimeSpan.FromSeconds(5);
    options.PushbackFrequency = TimeSpan.FromSeconds(10);
    options.CleanUpFrequency = TimeSpan.FromMinutes(5);
    options.Retention = TimeSpan.FromDays(7);
    options.Deadletter = true;
    options.DeadletterUniqueSchedules = false;
    options.UseCronSeconds = true;
    options.OnTaskFailedDelay = (_, retry) =>
        Task.FromResult(retry switch
        {
            2 => TimeSpan.FromSeconds(5),
            1 => TimeSpan.FromSeconds(60),
            _ => (TimeSpan?)null
        });
    options.OnTaskError = (_, exception) =>
    {
        sp.GetRequiredService<ILogger<ITaskProcessor>>()
            .LogError(exception, "Task Error");

        return Task.CompletedTask;
    };
});
builder.Services.AddTaskProcessorExecutor();

builder.Services.AddScoped<ISomeScopedService, SomeScopedService>();
builder.Services.AddScoped<FaultyService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.UseAuthorization();

app.MapControllers();

{
    var proc = app.Services.GetRequiredService<ITaskProcessor>();
    await using var scope = app.Services.CreateAsyncScope();
    var faulty = scope.ServiceProvider.GetRequiredService<FaultyService>();

    //foreach (var schedule in await proc.GetSchedulesAsync("core", 0, 100))
    //    await proc.CancelScheduleAsync(schedule.Id, "core");

    await proc.ResumeAsync();

    //var discarded = await proc.DiscardDeadTasksAsync("default");

    await proc.UpsertScheduleAsync(new ScheduleData
    {
        Id = "refresh",
        Tenant = "core",
        Queue = "default",
        Cron = "0 */2 * * * *",
        Timezone = "Etc/UTC",
        Unique = true
    }, () => faulty.DoFaultyStuff());
}

app.Run();