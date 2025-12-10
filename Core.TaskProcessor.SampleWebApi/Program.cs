using Core.TaskProcessor;
using Core.TaskProcessor.SampleWebApi.Services;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.AddTaskProcessor((sp, options) =>
{
    options.Redis = "localhost:6380,abortConnect=false";
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
    options.DeadletterSchedules = true;
    options.UseCronSeconds = true;
    options.OnTaskFailedDelay = (_, retry) =>
        Task.FromResult(retry switch
        {
            2 => TimeSpan.FromSeconds(5),
            1 => TimeSpan.FromSeconds(10),
            _ => (TimeSpan?)null
        });
    options.OnTaskError = (_, exception) =>
    {
        //sp.GetRequiredService<ILogger<ITaskProcessor>>()
        //    .LogError(exception, "Task Error");

        sp.GetRequiredService<ILogger<ITaskProcessor>>()
            .LogError(exception.Message);

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
    var svc = scope.ServiceProvider.GetRequiredService<ISomeScopedService>();

    foreach (var schedule in await proc.GetSchedulesAsync("core", 0, 100))
        await proc.CancelScheduleAsync(schedule.Id, "core");

    await proc.ResumeAsync();

    //var discarded = await proc.DiscardDeadTasksAsync("default");

    await proc.UpsertScheduleAsync(new ScheduleData
    {
        Id = "refresh",
        Tenant = "core",
        Queue = "default",
        Cron = "*/10 * * * * *",
        Timezone = "Etc/UTC",
        Unique = true
    }, () => svc.DoSomething("test"));

    //await proc.EnqueueTaskAsync("default", "core", () => faulty.DoFaultyStuff());
    //await proc.EnqueueTaskAsync("default", "core", () => svc.DoSomethingAsync("test", CancellationToken.None));
    //await proc.EnqueueTaskAsync("default", "core", () => svc.DoSomethingAsync("test-delay", CancellationToken.None), delayUntil: DateTimeOffset.UtcNow.AddSeconds(5));
}

app.Run();