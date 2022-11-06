using System.Linq.Expressions;

namespace Core.TaskProcessor;

public interface IBatchEnqueue
{
    void Enqueue(Expression<Func<Task>> methodCall, string? queue = null);
    void Enqueue(Expression<Action> methodCall, string? queue = null);
}

public interface IBatchContinue : IBatchEnqueue
{
    void ContinueWith(Expression<Func<Task>> methodCall, string? queue = null);
    void ContinueWith(Expression<Action> methodCall, string? queue = null);
}