using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Core.TaskProcessor;

public class RemoteExpressionExecutor : IRemoteExpressionExecutor
{
    public virtual byte[] Serialize(LambdaExpression methodCall, Type? explicitType)
    {
        var callExpression = methodCall.Body as MethodCallExpression;
        var type = explicitType ?? callExpression?.Method.DeclaringType;
        var method = callExpression!.Method;

        var info = new MethodCallInfo
        {
            Type = SerializeType(type!) ?? string.Empty,
            Method = method.Name,
            Signature = method.GetParameters().Select(x => SerializeType(x.ParameterType)).ToList()!,
            Arguments = callExpression.Arguments.Select(x => { return SerializeArgument(x); }).ToList()
        };

        return SerializeBinary(info);
    }

    public virtual async Task InvokeAsync(TaskContext ctx, Func<Type, object?> resolver)
    {
        var info = DeserializeBinary(ctx.Data, typeof(MethodCallInfo)) as MethodCallInfo;

        var type = DeserializeType(info.Type);
        var signature = info.Signature.Select(DeserializeType).ToArray();
        var method = type.GetMethod(info.Method, signature);

        if (method == null)
            throw new MissingMethodException(info.Type, info.Method);

        var args = new List<object?>();

        for (var i = 0; i < signature.Length; i++)
        {
            var sig = signature[i]!;
            var json = info.Arguments[i];

            args.Add(DeserializeArgument(ctx, sig, json));
        }

        var returnType = method.ReturnType;
        var instance = method.IsStatic ? null : resolver(type);

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

    protected virtual byte[] SerializeBinary(object obj)
    {
        return JsonSerializer.SerializeToUtf8Bytes(obj);
    }

    protected virtual string SerializeText(object obj)
    {
        return JsonSerializer.Serialize(obj, new JsonSerializerOptions
        {
            ReferenceHandler = ReferenceHandler.Preserve
        });
    }

    protected virtual object? DeserializeBinary(byte[] data, Type type)
    {
        return JsonSerializer.Deserialize(data, type);
    }

    protected virtual object? DeserializeText(string text, Type type)
    {
        return JsonSerializer.Deserialize(text, type, new JsonSerializerOptions
        {
            ReferenceHandler = ReferenceHandler.Preserve
        });
    }

    protected virtual object? DeserializeArgument(TaskContext ctx, Type sig, string? arg)
    {
        if (arg == null)
        {
            if (sig == typeof(CancellationToken))
                return ctx.CancelToken;
            if (sig == typeof(TaskContext))
                return ctx;
            return null;
        }

        return DeserializeText(arg, sig);
    }

    protected virtual string? SerializeArgument(Expression exp)
    {
        var arg = Evaluate(exp);

        if (arg is null or CancellationToken or TaskContext)
            return null;

        return SerializeText(arg);
    }

    protected virtual string? SerializeType(Type type)
    {
        if (type.IsPrimitive
            || type == typeof(CancellationToken)
            || type.Namespace == "System"
            || type.Namespace?.StartsWith("System.Collections") == true)
            return type.FullName;

        return type.AssemblyQualifiedName;
    }

    protected virtual Type? DeserializeType(string type)
    {
        // exact match
        var t1 = Type.GetType(type, false);

        if (t1 != null)
            return t1;

        // fuzzy match
        return Type.GetType(type, asn =>
        {
            asn.Version = null;
            asn.SetPublicKey(null);
            return Assembly.Load(asn);
        }, null, false);
    }

    // https://stackoverflow.com/questions/36861196/how-to-serialize-method-call-expression-with-arguments
    // https://stackoverflow.com/questions/2616638/access-the-value-of-a-member-expression
    protected virtual object? Evaluate(Expression? expr)
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
            case ExpressionType.ListInit:
            case ExpressionType.MemberInit:
            case ExpressionType.NewArrayInit:
                // just execute it...
                return Expression.Lambda(expr).Compile().DynamicInvoke();
            default:
                throw new NotSupportedException(expr.NodeType.ToString());
        }
    }

    protected class MethodCallInfo
    {
        public string Type { get; set; } = string.Empty;
        public string Method { get; set; } = string.Empty;
        public List<string> Signature { get; set; } = null!;
        public List<string?> Arguments { get; set; } = null!;
    }
}