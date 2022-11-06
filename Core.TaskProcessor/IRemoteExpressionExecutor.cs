using System.Linq.Expressions;

namespace Core.TaskProcessor;

/// <summary>
///     Serializes and invokes method call expressions
/// </summary>
public interface IRemoteExpressionExecutor
{
    byte[] Serialize(LambdaExpression methodCall, Type? explicitType = null);
    Task InvokeAsync(TaskContext ctx, Func<Type, object?> resolver);
}