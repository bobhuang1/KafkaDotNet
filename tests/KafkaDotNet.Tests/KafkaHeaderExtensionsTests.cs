using Confluent.Kafka;
using KafkaDotNet.Contracts;
using KafkaDotNet.Messaging;

namespace KafkaDotNet.Tests;

public sealed class KafkaHeaderExtensionsTests
{
    [Fact]
    public void String_headers_round_trip()
    {
        var headers = new Headers();
        headers.SetString("trace-id", "abc-123");

        Assert.Equal("abc-123", headers.GetString("trace-id"));
    }

    [Fact]
    public void Missing_header_reads_as_null()
    {
        var headers = new Headers();

        Assert.Null(headers.GetString(EventHeaders.TraceId));
        Assert.Null(headers.GetLong(EventHeaders.Attempt));
    }

    [Fact]
    public void Setting_a_header_twice_replaces_it()
    {
        var headers = new Headers();
        headers.SetString("k", "first");
        headers.SetString("k", "second");

        Assert.Equal("second", headers.GetString("k"));
    }

    [Fact]
    public void Setting_null_removes_the_header()
    {
        var headers = new Headers();
        headers.SetString("k", "value");
        headers.SetString("k", null);

        Assert.Null(headers.GetString("k"));
    }

    [Fact]
    public void Attempt_defaults_to_zero_for_a_fresh_record()
    {
        var headers = new Headers();

        Assert.Equal(0, headers.GetAttempt());
    }

    [Fact]
    public void Attempt_survives_a_round_trip()
    {
        var headers = new Headers();
        headers.SetLong(EventHeaders.Attempt, 3);

        Assert.Equal(3, headers.GetAttempt());
    }

    [Fact]
    public void NotBefore_round_trips_with_millisecond_precision()
    {
        var headers = new Headers();
        var when = DateTimeOffset.FromUnixTimeMilliseconds(1_900_000_000_000);

        headers.SetNotBefore(when);

        Assert.Equal(when, headers.GetNotBefore());
        Assert.Null(new Headers().GetNotBefore());
    }
}
