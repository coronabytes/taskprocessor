using System.Linq.Expressions;

namespace Core.TaskProcessor;

public interface IBatchEnqueue
{
    void Enqueue(Expression<Func<Task>> methodCall, string? queue = null, DateTimeOffset? delayUntil = null);
    void Enqueue(Expression<Action> methodCall, string? queue = null, DateTimeOffset? delayUntil = null);
}