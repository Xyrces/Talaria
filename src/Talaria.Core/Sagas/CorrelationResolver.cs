using System.Reflection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Sagas;

/// <summary>
/// Resolves the correlation identity from an incoming message to look up a saga state.
/// </summary>
/// <since>1.0.0</since>
public static class CorrelationResolver
{
    /// <summary>
    /// Resolves the correlation ID for a message payload by examining:
    /// 1. The correlation envelope header (if present).
    /// 2. Properties marked with [SagaCorrelation].
    /// 3. Properties named "CorrelationId" (case-insensitive).
    /// 4. Properties named "Id" (case-insensitive).
    /// 5. The first property whose name ENDS WITH "Id" (e.g. "OrderId", "AccountId").
    ///    Beware: this broad fallback can bind an unintended property — prefer
    ///    [SagaCorrelation] or an explicit correlateBy when in doubt.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type.</typeparam>
    /// <param name="message">The message payload.</param>
    /// <param name="headers">The message headers (used to read the correlation header).</param>
    /// <returns>The resolved correlation ID, or null when no candidate was found.</returns>
    public static string? Resolve<TMessage>(TMessage message, MessageHeaders headers) where TMessage : class
    {
        // 1. Check Headers
        if (headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var headerId) && !string.IsNullOrWhiteSpace(headerId))
        {
            return headerId;
        }

        var type = message.GetType();

        // 2. Check for [SagaCorrelation] attribute
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var explicitKeyProp = props.FirstOrDefault(p => p.GetCustomAttribute<Talaria.Core.Attributes.SagaCorrelationAttribute>() != null);
        if (explicitKeyProp != null)
        {
            return explicitKeyProp.GetValue(message)?.ToString();
        }

        // 3. Fallback to "CorrelationId"
        var conventionProp = props.FirstOrDefault(p => string.Equals(p.Name, "CorrelationId", StringComparison.OrdinalIgnoreCase));
        if (conventionProp != null)
        {
            return conventionProp.GetValue(message)?.ToString();
        }

        // 4. Fallback to "OrderId" / "RequestId" / "Id"
        var idProp = props.FirstOrDefault(p => string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase));
        if (idProp != null)
        {
            return idProp.GetValue(message)?.ToString();
        }

        idProp = props.FirstOrDefault(p => p.Name.EndsWith("Id", StringComparison.OrdinalIgnoreCase));
        if (idProp != null)
        {
            return idProp.GetValue(message)?.ToString();
        }

        return null;
    }
}
