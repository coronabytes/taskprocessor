using Cronos;
using Xunit;

namespace Core.TaskProcessor.Tests;

public class CronosTests
{
    [Fact]
    public void Parse()
    {
        CronExpression.Parse("0 * * * *", CronFormat.Standard);
        CronExpression.Parse("0 0 * * * *", CronFormat.IncludeSeconds);
    }
}