// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;

// namespace MyCoreApiTest
// {
//     public class KafkaDesignPatternsPipeline
//     {
        
//     }
// }
using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql; // PostgreSQL
using Polly;
using Polly.CircuitBreaker;

// Factory Pattern for Kafka Producer and Consumer
public static class PipelineFactory
{
    public static KafkaProducer CreateProducer(string bootstrapServers, string topic, string filePath)
        => new KafkaProducer(bootstrapServers, topic, filePath);

    public static KafkaConsumer CreateConsumer(string bootstrapServers, string topic, string groupId, string dbConn, string table)
        => new KafkaConsumer(bootstrapServers, topic, groupId, dbConn, table);
}

class KafkaProducer
{
    private string BootstrapServers;
    private string Topic;
    private string SourceFilePath;

    public KafkaProducer(string bootstrapServers, string topic, string sourceFilePath)
    {
        BootstrapServers = bootstrapServers;
        Topic = topic;
        SourceFilePath = sourceFilePath;
    }

    public async Task ProduceMessagesAsync()
    {
        var config = new ProducerConfig { BootstrapServers = BootstrapServers };
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var lines = File.ReadLines(SourceFilePath);
        
        var messageQueue = new BlockingCollection<string>(new ConcurrentQueue<string>(), 10000); // Thread-safe queue
        
        Parallel.ForEach(lines, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, line =>
        {
            messageQueue.Add(line);
        });
        
        await Task.Run(async () =>
        {
            foreach (var message in messageQueue.GetConsumingEnumerable())
            {
                await producer.ProduceAsync(Topic, new Message<Null, string> { Value = message });
                Console.WriteLine($"Produced: {message}");
            }
        });
    }
}

class KafkaConsumer
{
    private string BootstrapServers;
    private string Topic;
    private string GroupId;
    private string ConnectionString;
    private string TableName;
    private static readonly ConcurrentQueue<string> messageQueue = new();

    private static readonly AsyncCircuitBreakerPolicy circuitBreakerPolicy = Policy
        .Handle<Exception>()
        .CircuitBreakerAsync(5, TimeSpan.FromSeconds(30));

    private static readonly AsyncRetryPolicy retryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

    public KafkaConsumer(string bootstrapServers, string topic, string groupId, string connectionString, string tableName)
    {
        BootstrapServers = bootstrapServers;
        Topic = topic;
        GroupId = groupId;
        ConnectionString = connectionString;
        TableName = tableName;
    }

    public async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(cancellationToken);
                messageQueue.Enqueue(cr.Value);
                Console.WriteLine($"Consumed: {cr.Value}");

                if (messageQueue.Count >= 100)
                {
                    var batch = new List<string>();
                    while (messageQueue.TryDequeue(out var msg))
                    {
                        batch.Add(msg);
                    }

                    _ = Task.Run(() => BulkInsertToDatabaseAsync(batch));
                }

                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }

    private async Task BulkInsertToDatabaseAsync(List<string> records)
    {
        await circuitBreakerPolicy.ExecuteAsync(async () =>
        {
            await retryPolicy.ExecuteAsync(async () =>
            {
                await using var connection = new NpgsqlConnection(ConnectionString);
                await connection.OpenAsync();
                await using var writer = await connection.BeginBinaryImportAsync($"COPY {TableName} (data) FROM STDIN (FORMAT BINARY)");

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

class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length < 5)
        {
            Console.WriteLine("Usage: dotnet run <bootstrapServers> <topic> <sourceFilePath> <dbConnectionString> <tableName>");
            return;
        }

        string bootstrapServers = args[0];
        string topic = args[1];
        string sourceFilePath = args[2];
        string dbConnectionString = args[3];
        string tableName = args[4];

        var producer = PipelineFactory.CreateProducer(bootstrapServers, topic, sourceFilePath);
        var consumer = PipelineFactory.CreateConsumer(bootstrapServers, topic, "sample-group", dbConnectionString, tableName);

        using var cts = new CancellationTokenSource();
        var producerTask = producer.ProduceMessagesAsync();
        var consumerTask = consumer.ConsumeMessagesAsync(cts.Token);

        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
        };

        await Task.WhenAll(producerTask, consumerTask);
    }
}
