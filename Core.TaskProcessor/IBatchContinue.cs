using System.Linq.Expressions;

namespace Core.TaskProcessor;

public interface IBatchContinue : IBatchEnqueue
{
    void ContinueWith(Expression<Func<Task>> methodCall, string? queue = null);
    void ContinueWith(Expression<Action> methodCall, string? queue = null);
}