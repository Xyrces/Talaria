// SPDX-License-Identifier: Apache-2.0

using System.Reflection;
using System.Text.Json;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis.Tests;

/// <summary>
/// Unit tests for the Redis outbox store serialization. These do not need a live
/// Redis server because they exercise the private <c>Deserialize</c> helper directly.
/// </summary>
public class RedisOutboxStoreSerializationTests
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
            payloadJson = "\"hello-outbox\"",
            headers = new Dictionary<string, string> { [MessageHeaders.MessageIdKey] = "outbox-1" },
            createdAt = DateTimeOffset.UtcNow.AddSeconds(-5),
        };

        var json = JsonSerializer.Serialize(dto);
        var message = InvokeDeserialize(json);

        Assert.Equal(id, message.Id);
        Assert.Equal("orders-topic", message.Topic);
        Assert.Null(message.PartitionKey);
        Assert.Equal("outbox-1", message.Headers.MessageId);
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
            payloadJson = "\"hello-outbox\"",
            partitionKey = "order-partition-7",
            headers = new Dictionary<string, string> { [MessageHeaders.MessageIdKey] = "outbox-1" },
            createdAt = DateTimeOffset.UtcNow.AddSeconds(-5),
        };

        var json = JsonSerializer.Serialize(dto);
        var message = InvokeDeserialize(json);

        Assert.Equal("order-partition-7", message.PartitionKey);
    }

    [Fact]
    public void Serialize_ThenDeserialize_RoundTripsPartitionKey()
    {
        var message = new OutboxMessage(
            Guid.NewGuid(),
            "orders-topic",
            "System.String",
            "\"hello-outbox\"",
            new MessageHeaders { [MessageHeaders.MessageIdKey] = "outbox-1" },
            DateTimeOffset.UtcNow,
            "order-partition-7");

        var json = InvokeSerialize(message);
        var roundTripped = InvokeDeserialize(json);

        Assert.Equal(message.Id, roundTripped.Id);
        Assert.Equal(message.Topic, roundTripped.Topic);
        Assert.Equal(message.MessageType, roundTripped.MessageType);
        Assert.Equal(message.PayloadJson, roundTripped.PayloadJson);
        Assert.Equal(message.CreatedAt, roundTripped.CreatedAt);
        Assert.Equal("order-partition-7", roundTripped.PartitionKey);
        Assert.Equal("outbox-1", roundTripped.Headers.MessageId);
    }

    private static OutboxMessage InvokeDeserialize(string json)
    {
        var method = typeof(RedisOutboxStore).GetMethod(
            "Deserialize",
            BindingFlags.NonPublic | BindingFlags.Static,
            Type.DefaultBinder,
            [typeof(string)],
            null);

        Assert.NotNull(method);
        return (OutboxMessage)method!.Invoke(null, [json])!;
    }

    private static string InvokeSerialize(OutboxMessage message)
    {
        var method = typeof(RedisOutboxStore).GetMethod(
            "Serialize",
            BindingFlags.NonPublic | BindingFlags.Static,
            Type.DefaultBinder,
            [typeof(OutboxMessage)],
            null);

        Assert.NotNull(method);
        return (string)method!.Invoke(null, [message])!;
    }
}
