namespace Core.TaskProcessor.SampleWebApi.Services;

public interface ISomeScopedService
{
    Task DoSomethingAsync(string s, decimal d, CancellationToken token);
    Task DoSomethingAsync(string s, CancellationToken token);
    void DoSomething(string s);

    Task DoSomethingComplexAsync(string s, TaskContext ctx, CancellationToken token);
}