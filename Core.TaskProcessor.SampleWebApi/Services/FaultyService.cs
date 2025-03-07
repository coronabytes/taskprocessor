
namespace Core.TaskProcessor.SampleWebApi.Services;

public class FaultyService(ILogger<FaultyService> logger)
{
    public async Task DoFaultyStuff()
    {
        logger.LogInformation(nameof(DoFaultyStuff));
        await Task.Delay(200);
        throw new Exception("crash");
    }
}