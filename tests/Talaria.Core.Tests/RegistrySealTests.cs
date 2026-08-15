// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;

namespace Talaria.Core.Tests;

public class RegistrySealTests
{
    [Fact]
    public void TopicRegistry_Add_BeforeSeal_Works()
    {
        var registry = new TopicRegistry();

        registry.Add(new TopicRegistration
        {
            TopicName = "early",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        });

        Assert.Single(registry.Registrations);
        Assert.False(registry.IsSealed);
    }

    [Fact]
    public void TopicRegistry_Add_AfterSeal_ThrowsInvalidOperationException()
    {
        var registry = new TopicRegistry();
        registry.Seal();

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Add(new TopicRegistration
        {
            TopicName = "late",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        }));

        Assert.Contains("MapTopic", ex.Message);
        Assert.Contains("before the host runs", ex.Message);
        Assert.True(registry.IsSealed);
    }

    [Fact]
    public void TopicRegistry_Seal_IsIdempotent()
    {
        var registry = new TopicRegistry();
        registry.Seal();
        registry.Seal();

        Assert.Throws<InvalidOperationException>(() => registry.Add(new TopicRegistration
        {
            TopicName = "late",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        }));

        Assert.True(registry.IsSealed);
    }

    [Fact]
    public void SagaRegistry_Add_BeforeSeal_Works()
    {
        var registry = new SagaRegistry();

        registry.Add(new SagaRegistration
        {
            StateType = typeof(object),
        });

        Assert.Single(registry.Registrations);
        Assert.False(registry.IsSealed);
    }

    [Fact]
    public void SagaRegistry_Add_AfterSeal_ThrowsInvalidOperationException()
    {
        var registry = new SagaRegistry();
        registry.Seal();

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Add(new SagaRegistration
        {
            StateType = typeof(object),
        }));

        Assert.Contains("MapSaga", ex.Message);
        Assert.Contains("before the host runs", ex.Message);
        Assert.True(registry.IsSealed);
    }

    [Fact]
    public void SagaRegistry_Seal_IsIdempotent()
    {
        var registry = new SagaRegistry();
        registry.Seal();
        registry.Seal();

        Assert.Throws<InvalidOperationException>(() => registry.Add(new SagaRegistration
        {
            StateType = typeof(object),
        }));

        Assert.True(registry.IsSealed);
    }

    [Fact]
    public async Task TopicRegistry_ConcurrentAddAndSeal_ProducesConsistentState()
    {
        var registry = new TopicRegistry();
        var exceptions = new ConcurrentBag<Exception>();
        var addedCount = 0;

        var adders = Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() =>
            {
                for (int i = 0; i < 250; i++)
                {
                    try
                    {
                        registry.Add(new TopicRegistration
                        {
                            TopicName = $"topic-{i}",
                            MessageType = typeof(string),
                            Handler = (_, _, _) => Task.CompletedTask,
                        });
                        Interlocked.Increment(ref addedCount);
                    }
                    catch (InvalidOperationException)
                    {
                        // Expected race: seal happened before this add acquired the lock.
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
            }))
            .ToList();

        var sealer = Task.Run(() =>
        {
            for (int i = 0; i < 100; i++)
            {
                registry.Seal();
            }
        });

        await Task.WhenAll([.. adders, sealer]);

        Assert.Empty(exceptions);
        Assert.True(registry.IsSealed);
        Assert.Equal(addedCount, registry.Registrations.Count);
    }

    [Fact]
    public async Task SagaRegistry_ConcurrentAddAndSeal_ProducesConsistentState()
    {
        var registry = new SagaRegistry();
        var exceptions = new ConcurrentBag<Exception>();
        var addedCount = 0;

        var adders = Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() =>
            {
                for (int i = 0; i < 250; i++)
                {
                    try
                    {
                        registry.Add(new SagaRegistration
                        {
                            StateType = typeof(object),
                        });
                        Interlocked.Increment(ref addedCount);
                    }
                    catch (InvalidOperationException)
                    {
                        // Expected race: seal happened before this add acquired the lock.
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
            }))
            .ToList();

        var sealer = Task.Run(() =>
        {
            for (int i = 0; i < 100; i++)
            {
                registry.Seal();
            }
        });

        await Task.WhenAll([.. adders, sealer]);

        Assert.Empty(exceptions);
        Assert.True(registry.IsSealed);
        Assert.Equal(addedCount, registry.Registrations.Count);
    }
}
