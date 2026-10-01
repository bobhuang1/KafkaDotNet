using KafkaDotNet.Messaging;
using KafkaDotNet.OrderProcessor.Consumers;
using KafkaDotNet.OrderProcessor.Orders;

var builder = Host.CreateApplicationBuilder(args);

// Same registration as the API: identical producers, serializer and topic check.
builder.Services.AddKafkaMessaging(builder.Configuration);

// The business logic and the routing decision.
builder.Services.AddSingleton<IOrderHandler, SimulatedOrderHandler>();
builder.Services.AddSingleton<OrderPlacedProcessor>();
builder.Services.AddSingleton<ConsumedRecordHandler>();

// Three long-running loops: the pipeline, the retry schedule, and a second
// consumer group that demonstrates fan-out on the same topic.
builder.Services.AddHostedService<OrderPlacedConsumer>();
builder.Services.AddHostedService<RetryConsumer>();
builder.Services.AddHostedService<NotificationConsumer>();

var host = builder.Build();
host.Run();
