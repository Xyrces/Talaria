// SPDX-License-Identifier: Apache-2.0
using System.Linq.Expressions;
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

internal static class HandlerBinder
{
    public static Func<ConsumeContext<T>, Task> Bind<T>(Delegate handler, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (handler.GetInvocationList().Length != 1)
            throw new ArgumentException("Map one handler delegate per endpoint.", nameof(handler));
        if (handler.Method.ReturnType == typeof(void) && handler.Method.IsDefined(typeof(System.Runtime.CompilerServices.AsyncStateMachineAttribute), false))
            throw new ArgumentException("Async handlers must return Task or ValueTask so Talaria can await completion.", nameof(handler));
        var context = Expression.Parameter(typeof(ConsumeContext<T>), "context");
        var parameters = handler.Method.GetParameters();
        var serviceProbe = services.GetService<IServiceProviderIsService>();
        var args = new List<Expression>();
        var payloadCount = 0;
        foreach (var parameter in parameters)
        {
            var type = parameter.ParameterType;
            if (type == typeof(T))
            {
                payloadCount++;
                args.Add(Expression.Property(context, nameof(ConsumeContext<T>.Message)));
            }
            else if (type == typeof(ConsumeContext<T>)) args.Add(context);
            else if (type == typeof(CancellationToken)) args.Add(Expression.Property(context, nameof(ConsumeContext<T>.CancellationToken)));
            else
            {
                if (type.IsByRef || type.IsPointer || serviceProbe?.IsService(type) == false)
                    throw new ArgumentException($"Handler parameter '{parameter.Name}' ({type}) is not a registered service.", nameof(handler));
                args.Add(Expression.Convert(Expression.Call(typeof(ServiceProviderServiceExtensions),
                    nameof(ServiceProviderServiceExtensions.GetRequiredService), Type.EmptyTypes,
                    Expression.Property(context, nameof(ConsumeContext<T>.Services)), Expression.Constant(type)), type));
            }
        }
        if (payloadCount > 1) throw new ArgumentException("A handler may bind the message only once.", nameof(handler));
        var invoke = Expression.Invoke(Expression.Constant(handler), args);
        Expression body = handler.Method.ReturnType == typeof(void)
            ? Expression.Block(invoke, Expression.Constant(Task.CompletedTask))
            : handler.Method.ReturnType == typeof(ValueTask)
                ? Expression.Call(invoke, nameof(ValueTask.AsTask), Type.EmptyTypes)
                : handler.Method.ReturnType == typeof(Task)
                    ? invoke
                    : throw new ArgumentException("Message handlers must return void, Task, or ValueTask.", nameof(handler));
        return Expression.Lambda<Func<ConsumeContext<T>, Task>>(body, context).Compile();
    }
}
