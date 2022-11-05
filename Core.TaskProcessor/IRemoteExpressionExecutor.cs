using System.Linq.Expressions;

namespace Core.TaskProcessor;

public interface IRemoteExpressionExecutor
{
    byte[] Serialize(LambdaExpression methodCall, Type? explicitType = null);
    Task InvokeAsync(byte[] body, TaskContext ctx, CancellationToken token = default);
}