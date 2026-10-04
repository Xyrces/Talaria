using Confluent.Kafka;
using Xunit;

namespace Talaria.Transports.Kafka.Tests;

public class SettlementTrackerTests
{
    private static TopicPartitionOffset At(int partition, long offset) => new("orders", new Partition(partition), new Offset(offset));

    [Fact]
    public void PoisonRecordCannotCommitPastUnfinishedDelivery()
    {
        var tracker = new SettlementTracker();
        tracker.Delivered(At(0, 10));
        tracker.Delivered(At(0, 11));
        tracker.Settle(At(0, 11));
        Assert.Empty(tracker.Ready());
        tracker.Settle(At(0, 10));
        Assert.Equal(At(0, 12), Assert.Single(tracker.Ready()));
        tracker.Committed(At(0, 12));
        Assert.Empty(tracker.Ready());
    }

    [Fact]
    public void PhysicalOffsetGapsAndOtherPartitionsDoNotBlockSettledRecords()
    {
        var tracker = new SettlementTracker();
        tracker.Delivered(At(0, 10));
        tracker.Delivered(At(0, 15));
        tracker.Delivered(At(1, 0));
        tracker.Settle(At(0, 10));
        tracker.Settle(At(0, 15));
        Assert.Equal(At(0, 16), Assert.Single(tracker.Ready()));
    }
}
