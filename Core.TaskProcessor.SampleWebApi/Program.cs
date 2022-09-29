using Core.TaskProcessor;
using Core.TaskProcessor.SampleWebApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<ITaskProcessor>(new TaskProcessor(new TaskProcessorOptions
{
    Redis = "localhost:6379,abortConnect=false",
    Prefix = "{core}",
    Queues = new []{ "high", "default" },
    MaxWorkers = 4,
    Retries = 3,
    Invisibility = TimeSpan.FromMinutes(5),
    PollFrequency = TimeSpan.FromSeconds(10),
    Retention = TimeSpan.FromDays(7),
    OnTaskEnd = info => Task.CompletedTask
}));

builder.Services.AddHostedService<TaskService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.MapControllers();

app.Run();
