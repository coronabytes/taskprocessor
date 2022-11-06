using System.Linq.Expressions;

namespace Core.TaskProcessor;

public interface IBatch
{
    void Enqueue(Expression<Func<Task>> methodCall, string? queue = null);
    void ContinueWith(Expression<Func<Task>> methodCall, string? queue = null);
    void Enqueue(Expression<Action> methodCall, string? queue = null);
    void ContinueWith(Expression<Action> methodCall, string? queue = null);
}