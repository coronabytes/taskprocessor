using System.Diagnostics;

namespace Core.TaskProcessor.SampleWebApi.Services;

public class SomeScopedService : ISomeScopedService
{
    private readonly ILogger<SomeScopedService> _logger;

    public SomeScopedService(ILogger<SomeScopedService> logger)
    {
        _logger = logger;
    }

    public async Task DoSomethingAsync(string s, decimal d, CancellationToken token)
    {
        await Task.Delay(200, token).ConfigureAwait(false);
        _logger.LogInformation($"[{DateTime.UtcNow:HH:mm:ss}] DoSomethingAsync {s}, {d}");
    }

    public async Task DoSomethingAsync(string s, CancellationToken token)
    {
        await Task.Delay(100, token).ConfigureAwait(false);
        _logger.LogInformation($"[{DateTime.UtcNow:HH:mm:ss}] DoSomethingAsync {s}");
    }

    public void DoSomething(string s)
    {
        _logger.LogInformation($"[{DateTime.UtcNow:HH:mm:ss}] DoSomething {s}");
    }

    public async Task DoSomethingComplexAsync(string s, TaskContext ctx, CancellationToken token)
    {
        _logger.LogInformation($"[{DateTime.UtcNow:HH:mm:ss}] DoSomethingComplexAsync:Start {s}");
        await Task.Delay(TimeSpan.FromSeconds(10), token).ConfigureAwait(false);

        _logger.LogInformation($"[{DateTime.UtcNow:HH:mm:ss}] DoSomethingComplexAsync:Extend {s}");
        await ctx.ExtendLockAsync(TimeSpan.FromMinutes(10));
        
        await Task.Delay(TimeSpan.FromSeconds(5), token).ConfigureAwait(false);
        _logger.LogInformation($"[{DateTime.UtcNow:HH:mm:ss}] DoSomethingComplexAsync:Done {s}");
    }
}