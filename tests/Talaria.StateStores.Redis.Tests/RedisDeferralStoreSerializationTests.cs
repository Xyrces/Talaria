// SPDX-License-Identifier: Apache-2.0

using System.Reflection;
using System.Text.Json;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis.Tests;

/// <summary>
/// Unit tests for the Redis deferral store serialization. These do not need a live
/// Redis server because they exercise the private <c>Deserialize</c> helper directly.
/// </summary>
public class RedisDeferralStoreSerializationTests
{
    [Fact]
    public void Deserialize_OldEntryWithoutPartitionKey_DeserializesWithNullPartitionKey()
    {
        var id = Guid.NewGuid();
        var dto = new
        {
            id,
            topic = "orders-topic",
            messageType = "System.String",
            payloadJson = "\"hello-deferred\"",
            headers = new Dictionary<string, string> { [MessageHeaders.MessageIdKey] = "defer-1" },
            correlationId = "corr-123",
            attempt = 2,
            dueAt = DateTimeOffset.UtcNow.AddSeconds(-5),
        };

        var json = JsonSerializer.Serialize(dto);
        var message = InvokeDeserialize(json);

        Assert.Equal(id, message.Id);
        Assert.Equal("orders-topic", message.Topic);
        Assert.Null(message.PartitionKey);
        Assert.Equal("defer-1", message.Headers.MessageId);
        Assert.Equal("corr-123", message.CorrelationId);
    }

    [Fact]
    public void Deserialize_EntryWithPartitionKey_RoundTripsPartitionKey()
    {
        var id = Guid.NewGuid();
        var dto = new
        {
            id,
            topic = "orders-topic",
            messageType = "System.String",
            payloadJson = "\"hello-deferred\"",
            partitionKey = "order-partition-7",
            headers = new Dictionary<string, string> { [MessageHeaders.MessageIdKey] = "defer-1" },
            correlationId = "corr-123",
            attempt = 2,
            dueAt = DateTimeOffset.UtcNow.AddSeconds(-5),
        };

        var json = JsonSerializer.Serialize(dto);
        var message = InvokeDeserialize(json);

        Assert.Equal("order-partition-7", message.PartitionKey);
    }

    private static DeferredMessage InvokeDeserialize(string json)
    {
        var method = typeof(RedisDeferralStore).GetMethod(
            "Deserialize",
            BindingFlags.NonPublic | BindingFlags.Static,
            Type.DefaultBinder,
            [typeof(string)],
            null);

        Assert.NotNull(method);
        return (DeferredMessage)method!.Invoke(null, [json])!;
    }
}
