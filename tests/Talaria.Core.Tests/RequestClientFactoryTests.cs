// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Requesting;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

public class RequestClientFactoryTests
{
    [Fact]
    public void TwoFactories_HaveDistinctInboxTopics()
    {
        var options = new TalariaOptions { ApplicationName = "test-app" };
        var transport = new InMemoryTransport();

        var factory1 = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);
        var factory2 = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);

        Assert.NotEqual(factory1.InboxTopic, factory2.InboxTopic);
        Assert.StartsWith("test-app-replies-", factory1.InboxTopic);
        Assert.StartsWith("test-app-replies-", factory2.InboxTopic);
    }
}
