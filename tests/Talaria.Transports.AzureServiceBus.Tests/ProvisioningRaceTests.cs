// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Talaria.Core.Abstractions;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

public sealed class ProvisioningRaceTests
{
    private const string ConnectionString =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";

    [Fact]
    public async Task ConcurrentEnsureQueue_ConfirmsDuplicateRaceAndProvisionsDlq()
    {
        var admin = new FakeAdministration(queueBarrierEntity: "race");
        await using var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions { ConnectionString = ConnectionString }, admin);

        await Task.WhenAll(
            transport.EnsureEntityAsync("race", TopologyEntityKind.Queue),
            transport.EnsureEntityAsync("race", TopologyEntityKind.Queue)).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Contains("race", admin.Queues);
        Assert.Contains("race.dlq", admin.Queues);
        Assert.Equal(2, admin.CreateAttempts["race"]);
        Assert.Equal(2, admin.CreateAttempts["race.dlq"]);
    }

    [Fact]
    public async Task ConcurrentEnsureTopic_ConfirmsDuplicateRaceAndProvisionsDlq()
    {
        var admin = new FakeAdministration(topicBarrierEntity: "topic-race", queueBarrierEntity: "topic-race.dlq");
        await using var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions { ConnectionString = ConnectionString }, admin);

        await Task.WhenAll(
            transport.EnsureEntityAsync("topic-race", TopologyEntityKind.Topic),
            transport.EnsureEntityAsync("topic-race", TopologyEntityKind.Topic)).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Contains("topic-race", admin.Topics);
        Assert.Contains("topic-race.dlq", admin.Queues);
        Assert.Equal(2, admin.TopicCreateAttempts["topic-race"]);
        Assert.Equal(2, admin.CreateAttempts["topic-race.dlq"]);
    }

    [Fact]
    public async Task ConcurrentProvisionSubscription_ConfirmsDuplicateRace()
    {
        const string topic = "subscription-race-topic";
        const string subscription = "subscription-race-group";
        var admin = new FakeAdministration(subscriptionBarrier: (topic, subscription), existingTopics: [topic]);
        await using var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions { ConnectionString = ConnectionString }, admin);
        TopologyDeclaration[] declarations = [new(TopologyEntityKind.Subscription, subscription, topic)];

        await Task.WhenAll(
            transport.ProvisionAsync(declarations),
            transport.ProvisionAsync(declarations)).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Contains((topic, subscription), admin.Subscriptions);
        Assert.Equal(2, admin.SubscriptionCreateAttempts[(topic, subscription)]);
    }

    [Fact]
    public async Task EnsureQueue_PropagatesNonConflictCreateFailure()
    {
        var expected = new RequestFailedException(500, "management unavailable", "ServerBusy", null);
        var admin = new FakeAdministration { CreateFailure = _ => expected };
        await using var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions { ConnectionString = ConnectionString }, admin);

        var actual = await Assert.ThrowsAsync<RequestFailedException>(
            () => transport.EnsureEntityAsync("queue", TopologyEntityKind.Queue));

        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task EnsureQueue_DuplicateConflictWithoutVerifiedEntityStillFails()
    {
        var expected = new ServiceBusException(
            "Already exists", ServiceBusFailureReason.MessagingEntityAlreadyExists, "queue");
        var admin = new FakeAdministration
        {
            CreateFailure = _ => expected,
            AddBeforeCreateFailure = false,
        };
        await using var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions { ConnectionString = ConnectionString }, admin);

        var actual = await Assert.ThrowsAsync<ServiceBusException>(
            () => transport.EnsureEntityAsync("queue", TopologyEntityKind.Queue));

        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task EnsureQueue_RecognizesAdministrationAlreadyExistsResponse()
    {
        var admin = new FakeAdministration
        {
            CreateFailure = name => new RequestFailedException(409, "Already exists", "EntityAlreadyExists", null),
            AddBeforeCreateFailure = true,
        };
        await using var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions { ConnectionString = ConnectionString }, admin);

        await transport.EnsureEntityAsync("queue", TopologyEntityKind.Queue);

        Assert.Contains("queue", admin.Queues);
        Assert.Contains("queue.dlq", admin.Queues);
    }

    private sealed class FakeAdministration : IServiceBusAdministration
    {
        private readonly ConcurrentDictionary<string, EntityBarrier> _queueBarriers = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, EntityBarrier> _topicBarriers = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<(string Topic, string Name), EntityBarrier> _subscriptionBarriers = new();
        public ConcurrentDictionary<string, byte> Queues { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, byte> Topics { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<(string Topic, string Name), byte> Subscriptions { get; } = new();
        public ConcurrentDictionary<string, int> CreateAttempts { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<string, int> TopicCreateAttempts { get; } = new(StringComparer.Ordinal);
        public ConcurrentDictionary<(string Topic, string Name), int> SubscriptionCreateAttempts { get; } = new();
        public Func<string, Exception?>? CreateFailure { get; init; }
        public bool AddBeforeCreateFailure { get; init; } = true;

        public FakeAdministration(
            string? queueBarrierEntity = null,
            string? topicBarrierEntity = null,
            (string Topic, string Name)? subscriptionBarrier = null,
            IEnumerable<string>? existingTopics = null)
        {
            if (queueBarrierEntity is not null)
            {
                _queueBarriers[queueBarrierEntity] = new EntityBarrier();
                _queueBarriers[queueBarrierEntity + ".dlq"] = new EntityBarrier();
            }
            if (topicBarrierEntity is not null) _topicBarriers[topicBarrierEntity] = new EntityBarrier();
            if (subscriptionBarrier is { } key) _subscriptionBarriers[key] = new EntityBarrier();
            if (existingTopics is not null)
                foreach (var topic in existingTopics) Topics.TryAdd(topic, 0);
        }

        public async Task<bool> QueueExistsAsync(string name, CancellationToken ct)
        {
            if (await ReturnInitialMissingAsync(_queueBarriers, name, ct)) return false;
            return Queues.ContainsKey(name);
        }

        public Task CreateQueueAsync(CreateQueueOptions options, CancellationToken ct)
        {
            var name = options.Name;
            CreateAttempts.AddOrUpdate(name, 1, static (_, count) => count + 1);
            var failure = CreateFailure?.Invoke(name);
            if (failure is not null)
            {
                if (AddBeforeCreateFailure) Queues.TryAdd(name, 0);
                throw failure;
            }
            if (!Queues.TryAdd(name, 0))
                throw new ServiceBusException("Already exists", ServiceBusFailureReason.MessagingEntityAlreadyExists, name);
            return Task.CompletedTask;
        }

        public async Task<bool> TopicExistsAsync(string name, CancellationToken ct)
        {
            if (await ReturnInitialMissingAsync(_topicBarriers, name, ct)) return false;
            return Topics.ContainsKey(name);
        }

        public Task CreateTopicAsync(string name, CancellationToken ct)
        {
            TopicCreateAttempts.AddOrUpdate(name, 1, static (_, count) => count + 1);
            var failure = CreateFailure?.Invoke(name);
            if (failure is not null) throw failure;
            if (!Topics.TryAdd(name, 0))
                throw new ServiceBusException("Already exists", ServiceBusFailureReason.MessagingEntityAlreadyExists, name);
            return Task.CompletedTask;
        }

        public async Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct)
        {
            if (await ReturnInitialMissingAsync(_subscriptionBarriers, (topic, subscription), ct)) return false;
            return Subscriptions.ContainsKey((topic, subscription));
        }

        public Task CreateSubscriptionAsync(CreateSubscriptionOptions options, CancellationToken ct)
        {
            var key = (options.TopicName, options.SubscriptionName);
            SubscriptionCreateAttempts.AddOrUpdate(key, 1, static (_, count) => count + 1);
            var failure = CreateFailure?.Invoke(options.SubscriptionName);
            if (failure is not null) throw failure;
            if (!Subscriptions.TryAdd(key, 0))
                throw new ServiceBusException("Already exists", ServiceBusFailureReason.MessagingEntityAlreadyExists, options.SubscriptionName);
            return Task.CompletedTask;
        }

        private static async Task<bool> ReturnInitialMissingAsync<TKey>(
            ConcurrentDictionary<TKey, EntityBarrier> barriers,
            TKey key,
            CancellationToken ct) where TKey : notnull
        {
            if (!barriers.TryGetValue(key, out var barrier) || Interlocked.Increment(ref barrier.Checks) > 2) return false;
            if (Volatile.Read(ref barrier.Checks) == 2) barrier.InitialChecks.TrySetResult();
            await barrier.InitialChecks.Task.WaitAsync(ct);
            return true;
        }

        private sealed class EntityBarrier
        {
            public int Checks;
            public TaskCompletionSource InitialChecks { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}
