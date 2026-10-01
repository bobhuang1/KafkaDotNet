using KafkaDotNet.Messaging;
using KafkaDotNet.OrderApi;

var builder = WebApplication.CreateBuilder(args);

// One call wires the options, the JSON serializer, the producer, the topic
// bootstrapper and the publishers. The API and the worker share this, which is
// why they cannot disagree about the wire format or the topic names.
builder.Services.AddKafkaMessaging(builder.Configuration);

// Turns unhandled exceptions into RFC 9457 problem+json instead of a bare 500.
builder.Services.AddProblemDetails();

var app = builder.Build();

app.UseExceptionHandler();

app.MapOrderEndpoints();

app.Run();
