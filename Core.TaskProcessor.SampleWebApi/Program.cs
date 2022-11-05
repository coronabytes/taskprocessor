using Core.TaskProcessor;
using Core.TaskProcessor.SampleWebApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddTaskProcessor(new TaskProcessorOptions
{
    Redis = "localhost:6379,abortConnect=false",
    Prefix = "{core}",
    Queues = new[] { "high", "default" },
    MaxWorkers = 4,
    Retries = 3,
    Invisibility = TimeSpan.FromMinutes(5),
    PollFrequency = TimeSpan.FromSeconds(10),
    Retention = TimeSpan.FromDays(7),
    UseHostedService = true
});

builder.Services.AddScoped<ISomeScopedService, SomeScopedService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.MapControllers();

app.Run();