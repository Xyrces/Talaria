// SPDX-License-Identifier: Apache-2.0
using Confluent.Kafka;

namespace Talaria.Transports.Kafka;

/// <summary>Tracks delivered records, including gaps in Kafka's physical offsets.</summary>
internal sealed class SettlementTracker
{
    private readonly Dictionary<TopicPartition, SortedDictionary<long, bool>> _pending = new();

    public void Delivered(TopicPartitionOffset position)
    {
        if (!_pending.TryGetValue(position.TopicPartition, out var offsets))
            _pending[position.TopicPartition] = offsets = new();
        offsets[position.Offset.Value] = false;
    }

    public void Settle(TopicPartitionOffset position)
    {
        if (!_pending.TryGetValue(position.TopicPartition, out var offsets) || !offsets.ContainsKey(position.Offset.Value))
            throw new InvalidOperationException("Cannot settle a Kafka record that was not delivered by this consumer.");
        offsets[position.Offset.Value] = true;
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
        var offsets = _pending[next.TopicPartition];
        foreach (var offset in offsets.Keys.TakeWhile(x => x < next.Offset.Value).ToArray()) offsets.Remove(offset);
    }
}
