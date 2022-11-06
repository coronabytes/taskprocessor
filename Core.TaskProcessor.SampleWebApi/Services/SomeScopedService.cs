using System.Diagnostics;

namespace Core.TaskProcessor.SampleWebApi.Services;

public class SomeScopedService : ISomeScopedService
{
    public async Task DoSomethingAsync(string s, decimal d, CancellationToken token)
    {
        await Task.Delay(200, token).ConfigureAwait(false);
        Trace.WriteLine($"DoSomethingAsync {s}, {d}");
    }

    public async Task DoSomethingAsync(string s, CancellationToken token)
    {
        await Task.Delay(100, token).ConfigureAwait(false);
        Trace.WriteLine($"DoSomethingAsync {s}");
    }

    public void DoSomething(string s)
    {
        Trace.WriteLine($"DoSomething {s}");
    }

    public async Task DoSomethingComplexAsync(string s, TaskContext ctx, CancellationToken token)
    {
        await Task.Delay(TimeSpan.FromSeconds(10), token).ConfigureAwait(false);

        await ctx.ExtendLockAsync(TimeSpan.FromMinutes(10));
        
        await Task.Delay(TimeSpan.FromSeconds(5), token).ConfigureAwait(false);
    }
}