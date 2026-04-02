using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Redis;
using Xunit;

namespace Talaria.StateStores.Redis.Tests;

public class RedisStateStoreIntegrationTests : IAsyncLifetime
{
    private RedisContainer _redisContainer;
    private IServiceProvider _serviceProvider;

    public async Task InitializeAsync()
    {
        _redisContainer = new RedisBuilder()
            .WithImage("redis:7.2")
            .Build();

        await _redisContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseRedisStateStore(opts =>
        {
            opts.Configuration = _redisContainer.GetConnectionString();
            opts.KeyPrefix = "test-store:";
        });

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (_redisContainer != null)
        {
            await _redisContainer.DisposeAsync();
        }
    }

    [Fact]
    public async Task SaveAndLoad_SagaState_Successfully()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<DummyState>>();
        
        var correlationId = Guid.NewGuid().ToString("N");
        var state = new DummyState { Value = "Test 123", StepCount = 5 };

        // Save
        await stateStore.SaveAsync(correlationId, state);

        // Load
        var loaded = await stateStore.GetAsync(correlationId);

        Assert.NotNull(loaded);
        Assert.Equal("Test 123", loaded.Value);
        Assert.Equal(5, loaded.StepCount);

        // Delete
        await stateStore.DeleteAsync(correlationId);

        var loadedAfterDelete = await stateStore.GetAsync(correlationId);
        Assert.Null(loadedAfterDelete);
    }
    
    public class DummyState
    {
        public string Value { get; set; } = string.Empty;
        public int StepCount { get; set; }
    }
}
