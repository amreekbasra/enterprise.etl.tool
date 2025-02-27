// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;

// namespace MyCoreApiTest
// {
//     public class MSKafkaImplementNew
//     {
        
//     }
// }
using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Polly;
using Polly.CircuitBreaker;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<IKafkaProducerFactory, KafkaProducerFactory>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
builder.Services.AddControllers();
var app = builder.Build();
app.MapControllers();
app.Run();

[ApiController]
[Route("api/kafka")]
public class KafkaController : ControllerBase
{
    private readonly IKafkaProducerFactory _producerFactory;
    private readonly IKafkaConsumerFactory _consumerFactory;

    public KafkaController(IKafkaProducerFactory producerFactory, IKafkaConsumerFactory consumerFactory)
    {
        _producerFactory = producerFactory;
        _consumerFactory = consumerFactory;
    }

    [HttpPost("produce")]
    public async Task<IActionResult> ProduceMessages([FromBody] ProducerRequest request)
    {
        var producer = _producerFactory.CreateProducer(request.BootstrapServers);
        await producer.ProduceMessagesAsync(request.Topic, request.SourceFilePath);
        return Ok("Messages are being produced");
    }

    [HttpPost("consume")]
    public async Task<IActionResult> ConsumeMessages([FromBody] ConsumerRequest request)
    {
        var consumer = _consumerFactory.CreateConsumer(request.BootstrapServers, request.GroupId);
        var cts = new CancellationTokenSource();
        _ = consumer.ConsumeMessagesAsync(request.Topic, request.DbConnectionString, request.TableName, cts.Token);
        return Ok("Consumer started");
    }
}

public interface IKafkaProducer
{
    Task ProduceMessagesAsync(string topic, string sourceFilePath);
}

public interface IKafkaConsumer
{
    Task ConsumeMessagesAsync(string topic, string connectionString, string tableName, CancellationToken cancellationToken);
}

public class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<Null, string> _producer;

    public KafkaProducer(string bootstrapServers)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task ProduceMessagesAsync(string topic, string sourceFilePath)
    {
        var lines = File.ReadLines(sourceFilePath);
        var messageQueue = new BlockingCollection<string>(new ConcurrentQueue<string>(), 10000);
        Parallel.ForEach(lines, line => messageQueue.Add(line));

        await Task.Run(async () =>
        {
            foreach (var message in messageQueue.GetConsumingEnumerable())
            {
                await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
            }
        });
    }
}

public class KafkaConsumer : IKafkaConsumer
{
    private readonly IConsumer<Ignore, string> _consumer;

    private static readonly AsyncCircuitBreakerPolicy CircuitBreakerPolicy = Policy
        .Handle<Exception>()
        .CircuitBreakerAsync(5, TimeSpan.FromSeconds(30));

    private static readonly AsyncRetryPolicy RetryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

    public KafkaConsumer(string bootstrapServers, string groupId)
    {
        var config = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false };
        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
    }

    public async Task ConsumeMessagesAsync(string topic, string connectionString, string tableName, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        var messageQueue = new ConcurrentQueue<string>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var cr = _consumer.Consume(cancellationToken);
                messageQueue.Enqueue(cr.Value);

                if (messageQueue.Count >= 100)
                {
                    var batch = new List<string>();
                    while (messageQueue.TryDequeue(out var msg))
                    {
                        batch.Add(msg);
                    }
                    _ = Task.Run(() => BulkInsertToDatabaseAsync(batch, connectionString, tableName));
                }
                _consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException)
        {
            _consumer.Close();
        }
    }

    private async Task BulkInsertToDatabaseAsync(List<string> records, string connectionString, string tableName)
    {
        await CircuitBreakerPolicy.ExecuteAsync(async () =>
        {
            await RetryPolicy.ExecuteAsync(async () =>
            {
                await using var connection = new NpgsqlConnection(connectionString);
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
}

public interface IKafkaProducerFactory
{
    IKafkaProducer CreateProducer(string bootstrapServers);
}

public interface IKafkaConsumerFactory
{
    IKafkaConsumer CreateConsumer(string bootstrapServers, string groupId);
}

public class KafkaProducerFactory : IKafkaProducerFactory
{
    public IKafkaProducer CreateProducer(string bootstrapServers)
    {
        return new KafkaProducer(bootstrapServers);
    }
}

public class KafkaConsumerFactory : IKafkaConsumerFactory
{
    public IKafkaConsumer CreateConsumer(string bootstrapServers, string groupId)
    {
        return new KafkaConsumer(bootstrapServers, groupId);
    }
}
