var builder = DistributedApplication.CreateBuilder(args);

var redis = builder.AddRedis("redis");
var kafka = builder.AddKafka("kafka");

builder.AddProject<Projects.Talaria_Client_Api>("talaria-client")
    .WithReplicas(3)
    .WithReference(redis)
    .WithReference(kafka)
    .WaitFor(redis)
    .WaitFor(kafka);

builder.Build().Run();
