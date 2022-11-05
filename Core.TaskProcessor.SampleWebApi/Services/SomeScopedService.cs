using System.Diagnostics;

namespace Core.TaskProcessor.SampleWebApi.Services
{
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
    }
}
