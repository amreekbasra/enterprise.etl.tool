// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;

// namespace MyCoreApiTest
// {
//     public class WebapiPipelineSimple
//     {
        
//     }
// }
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Polly;
using Polly.CircuitBreaker;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<KafkaPipelineService>();
var app = builder.Build();

app.MapPost("/start-producer", async ([FromServices] KafkaPipelineService service, [FromBody] ProducerRequest request) =>
{
    return await service.StartProducer(request);
});

app.MapPost("/start-consumer", async ([FromServices] KafkaPipelineService service, [FromBody] ConsumerRequest request) =>
{
    return await service.StartConsumer(request);
});

app.MapGet("/status", ([FromServices] KafkaPipelineService service) =>
{
    return Results.Ok(service.GetStatus());
});

app.Run();

public class KafkaPipelineService
{
    private readonly ConcurrentDictionary<string, Task> _runningPipelines = new();
    private readonly ConcurrentQueue<string> _messageQueue = new();

    public async Task<IResult> StartProducer(ProducerRequest request)
    {
        if (_runningPipelines.ContainsKey(request.FilePath))
            return Results.BadRequest("Producer already running for this file.");

        var task = Task.Run(async () =>
        {
            var config = new ProducerConfig { BootstrapServers = request.BootstrapServers };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            var lines = File.ReadLines(request.FilePath);
            foreach (var line in lines)
            {
                await producer.ProduceAsync(request.Topic, new Message<Null, string> { Value = line });
            }
        });

        _runningPipelines[request.FilePath] = task;
        return Results.Ok($"Producer started for {request.FilePath}");
    }

    public async Task<IResult> StartConsumer(ConsumerRequest request)
    {
        if (_runningPipelines.ContainsKey(request.Topic))
            return Results.BadRequest("Consumer already running for this topic.");

        var task = Task.Run(async () =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = request.BootstrapServers,
                GroupId = request.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(request.Topic);

            while (true)
            {
                var cr = consumer.Consume();
                _messageQueue.Enqueue(cr.Value);

                if (_messageQueue.Count >= 100)
                {
                    var batch = new List<string>();
                    while (_messageQueue.TryDequeue(out var msg))
                        batch.Add(msg);

                    await BulkInsertToDatabaseAsync(batch, request.DbConnection, request.TableName);
                }

                consumer.Commit(cr);
            }
        });

        _runningPipelines[request.Topic] = task;
        return Results.Ok($"Consumer started for topic {request.Topic}");
    }

    private async Task BulkInsertToDatabaseAsync(List<string> records, string dbConnection, string tableName)
    {
        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

        var circuitBreakerPolicy = Policy
            .Handle<Exception>()
            .CircuitBreakerAsync(5, TimeSpan.FromSeconds(30));

        await circuitBreakerPolicy.ExecuteAsync(async () =>
        {
            await retryPolicy.ExecuteAsync(async () =>
            {
                await using var connection = new NpgsqlConnection(dbConnection);
                await connection.OpenAsync();
                await using var writer = await connection.BeginBinaryImportAsync($"COPY {tableName} (data) FROM STDIN (FORMAT BINARY)");

                foreach (var record in records)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(record);
                }

                await writer.CompleteAsync();
            });
        });
    }

    public object GetStatus()
    {
        return new { RunningPipelines = _runningPipelines.Keys.ToList() };
    }
}

public class ProducerRequest
{
    public string BootstrapServers { get; set; }
    public string Topic { get; set; }
    public string FilePath { get; set; }
}

public class ConsumerRequest
{
    public string BootstrapServers { get; set; }
    public string Topic { get; set; }
    public string GroupId { get; set; }
    public string DbConnection { get; set; }
    public string TableName { get; set; }
}


// curl -X POST "http://localhost:5000/start-producer" -H "Content-Type: application/json" \
// -d '{"bootstrapServers":"localhost:9092","topic":"my-topic","filePath":"/data/file1.csv"}'


// curl -X POST "http://localhost:5000/start-consumer" -H "Content-Type: application/json" \
// -d '{"bootstrapServers":"localhost:9092","topic":"my-topic","groupId":"group-1","dbConnection":"Host=localhost;Database=mydb;Username=user;Password=pass","tableName":"table1"}'


// #!/bin/bash

// dotnet run "localhost:9092" "topic1" "/data/file1.csv" "Host=localhost;Database=mydb;Username=user;Password=pass" "table1" &
// dotnet run "localhost:9092" "topic2" "/data/file2.csv" "Host=localhost;Database=mydb;Username=user;Password=pass" "table2" &
// dotnet run "localhost:9092" "topic3" "/data/file3.csv" "Host=localhost;Database=mydb;Username=user;Password=pass" "table3" &

// wait  # Ensures script waits for all processes to finish
