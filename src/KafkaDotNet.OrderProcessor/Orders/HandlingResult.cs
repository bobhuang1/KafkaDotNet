namespace KafkaDotNet.OrderProcessor.Orders;

/// <summary>
/// What the handler did with one order. The distinction that matters is
/// <see cref="Permanent"/>: a transient failure is worth retrying, a permanent one
/// never will be, so retrying it just burns attempts before the dead letter.
/// </summary>
/// <param name="Succeeded">The work completed.</param>
/// <param name="Permanent">
/// The failure cannot be fixed by trying again (a SKU that no longer exists, a
/// malformed address). Such messages skip the retry schedule entirely.
/// </param>
/// <param name="Reason">Human-readable explanation, carried onto the follow-up record.</param>
public sealed record HandlingResult(bool Succeeded, bool Permanent, string? Reason)
{
    /// <summary>The order was processed.</summary>
    public static HandlingResult Success() => new(true, Permanent: false, Reason: null);

    /// <summary>Try again later — the cause may clear on its own.</summary>
    public static HandlingResult Transient(string reason) => new(false, Permanent: false, reason);

    /// <summary>Do not retry; record the outcome and move on.</summary>
    public static HandlingResult PermanentFailure(string reason) => new(false, Permanent: true, reason);
}

/// <summary>
/// The business logic for one order. In a real system this would call inventory
/// and the payment provider; here it is a deterministic simulation so the sample
/// exercises every branch without external dependencies.
/// </summary>
public interface IOrderHandler
{
    /// <summary>Handle an order. <paramref name="attempt"/> is zero on the first delivery.</summary>
    Task<HandlingResult> HandleAsync(Contracts.OrderPlaced order, int attempt, CancellationToken cancellationToken);
}
