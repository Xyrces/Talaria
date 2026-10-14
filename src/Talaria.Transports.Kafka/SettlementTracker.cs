// SPDX-License-Identifier: Apache-2.0
using Confluent.Kafka;

namespace Talaria.Transports.Kafka;

/// <summary>Tracks delivered records, including gaps in Kafka's physical offsets.</summary>
internal sealed class SettlementTracker
{
    private readonly Dictionary<TopicPartition, SortedDictionary<long, bool>> _pending = new();
    private readonly Dictionary<TopicPartition, long> _committedUpTo = new();
    private readonly HashSet<TopicPartition> _revoked = new();

    public void Delivered(TopicPartitionOffset position)
    {
        _revoked.Remove(position.TopicPartition);
        if (!_pending.TryGetValue(position.TopicPartition, out var offsets))
            _pending[position.TopicPartition] = offsets = new();
        offsets[position.Offset.Value] = false;
    }

    public bool IsPending(TopicPartitionOffset position)
        => _pending.TryGetValue(position.TopicPartition, out var offsets) && offsets.ContainsKey(position.Offset.Value);

    public void Settle(TopicPartitionOffset position)
    {
        if (_pending.TryGetValue(position.TopicPartition, out var offsets) && offsets.ContainsKey(position.Offset.Value))
        {
            offsets[position.Offset.Value] = true;
            return;
        }

        if (_committedUpTo.TryGetValue(position.TopicPartition, out var committedNext) && position.Offset.Value < committedNext)
            return;

        if (_revoked.Contains(position.TopicPartition))
            return;

        throw new InvalidOperationException("Cannot settle a Kafka record that was not delivered by this consumer.");
    }

    public void Revoke(IEnumerable<TopicPartition> partitions)
    {
        foreach (var partition in partitions)
        {
            _pending.Remove(partition);
            _committedUpTo.Remove(partition);
            _revoked.Add(partition);
        }
    }

    public IReadOnlyList<TopicPartitionOffset> Ready()
    {
        var ready = new List<TopicPartitionOffset>();
        foreach (var (partition, offsets) in _pending)
        {
            long? last = null;
            foreach (var (offset, settled) in offsets)
            {
                if (!settled) break;
                last = offset;
            }
            if (last.HasValue) ready.Add(new(partition, new Offset(last.Value + 1)));
        }
        return ready;
    }

    public void Committed(TopicPartitionOffset next)
    {
        _committedUpTo[next.TopicPartition] = next.Offset.Value;
        if (!_pending.TryGetValue(next.TopicPartition, out var offsets)) return;
        foreach (var offset in offsets.Keys.TakeWhile(x => x < next.Offset.Value).ToArray()) offsets.Remove(offset);
    }
}

