// SPDX-License-Identifier: AGPL-3.0-or-later

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
    }

    [Fact]
    public void TopicRegistry_Seal_IsIdempotent()
    {
        var registry = new TopicRegistry();
        registry.Seal();
        registry.Seal();

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Add(new TopicRegistration
        {
            TopicName = "late",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        }));

        Assert.NotNull(ex);
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
    }

    [Fact]
    public void SagaRegistry_Seal_IsIdempotent()
    {
        var registry = new SagaRegistry();
        registry.Seal();
        registry.Seal();

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Add(new SagaRegistration
        {
            StateType = typeof(object),
        }));

        Assert.NotNull(ex);
    }
}
