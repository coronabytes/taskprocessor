using Cronos;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Core.TaskProcessor.Tests;

public class CronosTests
{
    private readonly ITestOutputHelper _output;
    private readonly TaskProcessor _processor;

    public CronosTests(ITestOutputHelper output)
    {
        _output = output;
       
    }

    [Fact]
    public void Parse()
    {
        CronExpression.Parse("0 * * * *", CronFormat.Standard);
        CronExpression.Parse("0 0 * * * *", CronFormat.IncludeSeconds);
    }
}