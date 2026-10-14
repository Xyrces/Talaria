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

    [Fact]
    public void Given_CommittedRecord_When_SettledAgain_Then_IsIdempotentAndDoesNotThrow()
    {
        var tracker = new SettlementTracker();
        tracker.Delivered(At(0, 10));
        tracker.Settle(At(0, 10));
        Assert.Equal(At(0, 11), Assert.Single(tracker.Ready()));
        tracker.Committed(At(0, 11));

        tracker.Settle(At(0, 10));
        Assert.Empty(tracker.Ready());
    }

    [Fact]
    public void Given_RevokedPartitionWithUnsettledOffset_When_PartitionIsRevokedAndReassigned_Then_StaleOffsetDoesNotBlockNewDeliveries()
    {
        var tracker = new SettlementTracker();
        tracker.Delivered(At(0, 10));
        tracker.Delivered(At(1, 5));
        tracker.Settle(At(1, 5));

        tracker.Revoke([new TopicPartition("orders", new Partition(0))]);
        tracker.Settle(At(0, 10));

        tracker.Delivered(At(0, 20));
        tracker.Settle(At(0, 20));

        var ready = tracker.Ready();
        Assert.Contains(At(0, 21), ready);
        Assert.Contains(At(1, 6), ready);
    }
}

