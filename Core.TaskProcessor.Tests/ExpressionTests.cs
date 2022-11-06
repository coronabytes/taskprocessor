using System.Diagnostics;
using System.Linq.Expressions;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Core.TaskProcessor.Tests;

public class ExpressionTests
{
    private readonly IRemoteExpressionExecutor _executor = new RemoteExpressionExecutor();
    private readonly ITestOutputHelper _output;
    private readonly IServiceProvider _serviceProvider;

    public ExpressionTests(ITestOutputHelper output)
    {
        _output = output;
        _serviceProvider = new ServiceCollection()
            .AddSingleton<ISampleService, SampleService>()
            .BuildServiceProvider();
    }

    public static async Task SomeStaticFunction(string s, decimal d, int i, CancellationToken token)
    {
        await Task.Delay(0, token);
    }

    private async Task Execute(Expression<Func<Task>> methodCall)
    {
        var sw = new Stopwatch();
        sw.Start();

        var info = _executor.Serialize(methodCall);
        _output.WriteLine($"#1 {sw.ElapsedMilliseconds}");
        await using var scope = _serviceProvider.CreateAsyncScope();
        await _executor.InvokeAsync(new TaskContext
        {
            Data = info
        }, type => scope.ServiceProvider.GetRequiredService(type))
            .ConfigureAwait(false);
        _output.WriteLine($"#2 {sw.ElapsedMilliseconds}");
    }

    [Fact]
    public async Task Serialize()
    {
        var s = "hello";
        var sampleService = _serviceProvider.GetRequiredService<ISampleService>();
        await Execute(() => sampleService.SomeFunction(s, 123.45m, 1337, CancellationToken.None));
        var list = new List<string> {"1", "2"};
        await Execute(() => sampleService.SomeFunction(list));
        await Execute(() => sampleService.SomeFunction(new List<string> {"1", "2"}));
        await Execute(() => sampleService.SomeFunction(new SomeData
        {
            Name = "Test",
            Value = 1337.5m,
            Tags = new HashSet<string> {"a", "b", "c"}
        }));
        await Execute(() => SomeStaticFunction(s, 123.45m, 1337, CancellationToken.None));
    }

    private interface ISampleService
    {
        Task SomeFunction(string s, decimal d, int i, CancellationToken token);
        Task SomeFunction(List<string> list);
        Task SomeFunction(SomeData data);
    }

    private class SampleService : ISampleService
    {
        public async Task SomeFunction(string s, decimal d, int i, CancellationToken token)
        {
            await Task.Delay(0, token);
        }

        public async Task SomeFunction(List<string> list)
        {
            await Task.Delay(0);
        }

        public async Task SomeFunction(SomeData data)
        {
            await Task.Delay(0);
        }
    }

    private class SomeData
    {
        public string Name { get; set; }
        public decimal Value { get; set; }
        public HashSet<string> Tags { get; set; }
    }
}