using System.Linq.Expressions;

namespace Core.TaskProcessor.SampleWebApi.Services;

public interface ITaskService
{
    Task<string> EnqueueAsync(Expression<Func<Task>> methodCall, string queue);
}