namespace Talaria.Specs.Messages;

public class OrderPlacedSaga
{
    public string CorrelationId { get; set; } = string.Empty;
}

public class OrderBilledSaga
{
    public string OrderId { get; set; } = string.Empty;
}

public class OrderCompletedSaga
{
    public string Id { get; set; } = string.Empty;
}

public class OrderSagaState
{
    public string Id { get; set; } = string.Empty;
    public bool Placed { get; set; }
    public bool Billed { get; set; }
}
