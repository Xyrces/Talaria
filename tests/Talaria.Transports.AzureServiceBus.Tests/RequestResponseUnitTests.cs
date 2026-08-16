// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Requesting;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Unit-level request/response tests for ASB-bound composition in <c>MapRequest</c> scenarios that do not require the ASB emulator.
/// </summary>
public class RequestResponseUnitTests
    {
        private sealed record Ping(string Value);
        private sealed record Pong(string Echo);

        private sealed class FaultingResponder : IRequestConsumer<Ping, Pong>
        {
            public Task<Pong> ConsumeAsync(ConsumeContext<Ping> context, CancellationToken ct = default)
                => throw new InvalidOperationException("responder failure");
        }

        private sealed class RecordingProvisioner : ITopologyProvisioner
        {
            public List<TopologyDeclaration> Declarations { get; } = new();

            public Task ProvisionAsync(IEnumerable<TopologyDeclaration> declarations, CancellationToken ct = default)
            {
                Declarations.AddRange(declarations);
                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task RequestClientFactory_WithProvisioner_ProvisionsInboxQueue()
        {
            var transport = new Talaria.Transports.InMemory.InMemoryTransport();
            var options = new TalariaOptions { ApplicationName = "asb-rr-test" };
            var provisioner = new RecordingProvisioner();
            var loggerFactory = Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;

            var factory = new RequestClientFactory(transport, options, loggerFactory, provisioner);
            await using (factory.ConfigureAwait(false))
            {
                var client = factory.CreateClient<Ping>("ping.topic");

                // Trigger factory initialization (and provisioning) by sending a request.
                _ = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
                    client.GetResponseAsync<Pong>(new Ping("init"), CancellationToken.None));

                var declaration = Assert.Single(provisioner.Declarations);
                Assert.Equal(TopologyEntityKind.Queue, declaration.Kind);
                Assert.StartsWith("asb-rr-test-replies-", declaration.Name);
            }
        }

        [Fact]
        public async Task RequestClient_MapsResponderFaultToRequestFaultException()
        {
            var transport = new Talaria.Transports.InMemory.InMemoryTransport();
            var options = new TalariaOptions
            {
                ApplicationName = "asb-rr-test",
                IncludeExceptionDetailsInDlq = true,
            };
            var loggerFactory = Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;

            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
                .AddSingleton(transport)
                .AddScoped<FaultingResponder>()
                .BuildServiceProvider();

            var registry = new TopicRegistry();
            registry.MapRequest<Ping, FaultingResponder, Pong>("ping.fault.topic");

            var listener = new TalariaListener(
                transport,
                registry,
                new Talaria.Core.Sagas.SagaRegistry(),
                options,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<TalariaListener>.Instance,
                services);

            var factory = new RequestClientFactory(transport, options, loggerFactory);
            await using (factory.ConfigureAwait(false))
            {
                var client = factory.CreateClient<Ping>("ping.fault.topic");
                await listener.StartAsync();

                var ex = await Assert.ThrowsAsync<RequestFaultException>(() =>
                    client.GetResponseAsync<Pong>(new Ping("fault")));

                Assert.Equal(typeof(InvalidOperationException).FullName, ex.ResponderExceptionType);
                Assert.Equal("responder failure", ex.ResponderMessage);

                await listener.StopAsync();
            }
        }
    }
