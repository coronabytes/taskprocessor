﻿using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Core.TaskProcessor;

public class RemoteExpressionExecutor : IRemoteExpressionExecutor
{
    public byte[] Serialize(LambdaExpression methodCall, Type? explicitType)
    {
        var callExpression = methodCall.Body as MethodCallExpression;
        var type = explicitType ?? callExpression?.Method.DeclaringType;
        var method = callExpression!.Method;

        return JsonSerializer.SerializeToUtf8Bytes(new MethodCallInfo
        {
            Type = type!.AssemblyQualifiedName!,
            Method = method.Name,
            Signature = method.GetParameters().Select(x => x.ParameterType.FullName).ToList()!,
            Arguments = callExpression.Arguments.Select(x =>
            {
                var arg = Evaluate(x);

                if (arg is null or CancellationToken or TaskContext)
                    return null;

                return JsonSerializer.Serialize(arg);
            }).ToList()
        });
    }

    public async Task InvokeAsync(TaskContext ctx, IServiceScope scope)
    {
        var info = JsonSerializer.Deserialize<MethodCallInfo>(ctx.Data);

        var type = Type.GetType(info.Type);
        var signature = info.Signature.Select(x => Type.GetType(x)).ToArray();
        var method = type.GetMethod(info.Method, signature);

        if (method == null)
            throw new MissingMethodException(info.Type, info.Method);

        var args = new List<object?>();

        for (var i = 0; i < signature.Length; i++)
        {
            var sig = signature[i];
            var json = info.Arguments[i];

            if (json == null)
            {
                if (sig == typeof(CancellationToken))
                    args.Add(ctx.CancelToken);
                else if (sig == typeof(TaskContext))
                    args.Add(ctx);
                else
                    args.Add(null);
            }
            else
            {
                args.Add(JsonSerializer.Deserialize(json, sig));
            }
        }

        var returnType = method.ReturnType;

        if (!method.IsStatic)
        {
            var instance = scope.ServiceProvider.GetRequiredService(type);

            if (returnType == typeof(Task))
            {
                dynamic awaitable = method.Invoke(instance, args.ToArray())!;
                await awaitable;
            }
            else if (returnType == typeof(void))
            {
                method.Invoke(instance, args.ToArray());
            }
        }
        else
        {
            if (returnType == typeof(Task))
            {
                dynamic awaitable = method.Invoke(null, args.ToArray())!;
                await awaitable;
            }
            else if (returnType == typeof(void))
            {
                method.Invoke(null, args.ToArray());
            }
        }
    }

    // https://stackoverflow.com/questions/36861196/how-to-serialize-method-call-expression-with-arguments
    private static object? Evaluate(Expression? expr)
    {
        if (expr == null)
            return null;

        switch (expr.NodeType)
        {
            case ExpressionType.Constant:
                return ((ConstantExpression)expr).Value;
            case ExpressionType.MemberAccess:
                var me = (MemberExpression)expr;
                var target = Evaluate(me.Expression);
                switch (me.Member.MemberType)
                {
                    case MemberTypes.Field:
                        return ((FieldInfo)me.Member).GetValue(target);
                    case MemberTypes.Property:
                        return ((PropertyInfo)me.Member).GetValue(target, null);
                    default:
                        throw new NotSupportedException(me.Member.MemberType.ToString());
                }
            case ExpressionType.New:
                return ((NewExpression)expr).Constructor
                    .Invoke(((NewExpression)expr).Arguments.Select(Evaluate).ToArray());
            default:
                throw new NotSupportedException(expr.NodeType.ToString());
        }
    }

    private class MethodCallInfo
    {
        public string Type { get; set; } = string.Empty;
        public string Method { get; set; } = string.Empty;
        public List<string> Signature { get; set; } = null!;
        public List<string?> Arguments { get; set; } = null!;
    }
}