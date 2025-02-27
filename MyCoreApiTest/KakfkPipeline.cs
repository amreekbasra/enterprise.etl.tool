// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;

// namespace MyCoreApiTest
// {
//     public class KakfkPipeline
//     {
        
//     }
 using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

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

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            foreach (var line in File.ReadLines(SourceFilePath))
            {
                var message = new Message<Null, string> { Value = line };
                await producer.ProduceAsync(Topic, message);
                Console.WriteLine($"Produced: {message.Value}");
            }
        }
    }
}

class KafkaConsumer
{
    private string BootstrapServers;
    private string Topic;
    private string GroupId;
    private string DestinationStoragePath;

    public KafkaConsumer(string bootstrapServers, string topic, string groupId, string destinationStoragePath)
    {
        BootstrapServers = bootstrapServers;
        Topic = topic;
        GroupId = groupId;
        DestinationStoragePath = destinationStoragePath;
    }

    public void ConsumeMessages()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(Topic);

            try
            {
                using (StreamWriter writer = new StreamWriter(DestinationStoragePath, true))
                {
                    while (true)
                    {
                        var cr = consumer.Consume(CancellationToken.None);
                        writer.WriteLine(cr.Value);
                        Console.WriteLine($"Consumed: {cr.Value}");
                        consumer.Commit(cr);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length < 4)
        {
            Console.WriteLine("Usage: dotnet run <bootstrapServers> <topic> <sourceFilePath> <destinationStoragePath>");
            return;
        }

        string bootstrapServers = args[0];
        string topic = args[1];
        string sourceFilePath = args[2];
        string destinationStoragePath = args[3];

        var producer = new KafkaProducer(bootstrapServers, topic, sourceFilePath);
        var consumer = new KafkaConsumer(bootstrapServers, topic, "sample-group", destinationStoragePath);

        var producerTask = producer.ProduceMessagesAsync();
        var consumerTask = Task.Run(() => consumer.ConsumeMessages());

        await Task.WhenAll(producerTask, consumerTask);
    }
}

//}

using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql; // PostgreSQL

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

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var lines = File.ReadLines(SourceFilePath).AsParallel();
            await Task.WhenAll(lines.Select(async line =>
            {
                var message = new Message<Null, string> { Value = line };
                await producer.ProduceAsync(Topic, message);
                Console.WriteLine($"Produced: {message.Value}");
            }));
        }
    }
}

class KafkaConsumer
{
    private string BootstrapServers;
    private string Topic;
    private string GroupId;
    private string ConnectionString;
    private string TableName;

    public KafkaConsumer(string bootstrapServers, string topic, string groupId, string connectionString, string tableName)
    {
        BootstrapServers = bootstrapServers;
        Topic = topic;
        GroupId = groupId;
        ConnectionString = connectionString;
        TableName = tableName;
    }

    public void ConsumeMessages()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(Topic);
            var buffer = new ConcurrentBag<string>();

            try
            {
                while (true)
                {
                    var cr = consumer.Consume(CancellationToken.None);
                    buffer.Add(cr.Value);
                    Console.WriteLine($"Consumed: {cr.Value}");

                    if (buffer.Count >= 100) // Batch insert
                    {
                        InsertIntoDatabase(buffer.ToList());
                        buffer = new ConcurrentBag<string>();
                    }

                    consumer.Commit(cr);
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }

    private void InsertIntoDatabase(List<string> records)
    {
        using (var connection = new NpgsqlConnection(ConnectionString))
        {
            connection.Open();
            using (var transaction = connection.BeginTransaction())
            {
                using (var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;
                    command.CommandText = $"INSERT INTO {TableName} (data) VALUES (@data)";
                    command.Parameters.Add(new NpgsqlParameter("@data", NpgsqlTypes.NpgsqlDbType.Text));

                    foreach (var record in records)
                    {
                        command.Parameters["@data"].Value = record;
                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
        }
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

        var producer = new KafkaProducer(bootstrapServers, topic, sourceFilePath);
        var consumer = new KafkaConsumer(bootstrapServers, topic, "sample-group", dbConnectionString, tableName);

        var producerTask = producer.ProduceMessagesAsync();
        var consumerTask = Task.Run(() => consumer.ConsumeMessages());

        await Task.WhenAll(producerTask, consumerTask);
    }
}

class NewImprovements{
public async Task ProduceMessagesAsync()
{
    var config = new ProducerConfig { BootstrapServers = BootstrapServers };
    
    using (var producer = new ProducerBuilder<Null, string>(config).Build())
    {
        var lines = File.ReadLines(SourceFilePath);

        await Parallel.ForEachAsync(lines, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, async (line, _) =>
        {
            var message = new Message<Null, string> { Value = line };
            await producer.ProduceAsync(Topic, message);
            Console.WriteLine($"Produced: {message.Value}");
        });
    }
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

    using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
    {
        consumer.Subscribe(Topic);
        var buffer = new ConcurrentBag<string>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(cancellationToken);
                buffer.Add(cr.Value);
                Console.WriteLine($"Consumed: {cr.Value}");

                if (buffer.Count >= 100) // Batch insert
                {
                    var batch = buffer.ToList();
                    buffer.Clear();

                    // Non-blocking database insert
                    _ = Task.Run(() => InsertIntoDatabase(batch));
                }

                consumer.Commit(cr);
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
}
private async Task InsertIntoDatabase(List<string> records)
{
    using (var connection = new NpgsqlConnection(ConnectionString))
    {
        await connection.OpenAsync();

        using (var transaction = await connection.BeginTransactionAsync())
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = $"INSERT INTO {TableName} (data) VALUES (@data)";
                command.Parameters.Add(new NpgsqlParameter("@data", NpgsqlTypes.NpgsqlDbType.Text));

                foreach (var record in records)
                {
                    command.Parameters["@data"].Value = record;
                    await command.ExecuteNonQueryAsync();
                }
            }
            await transaction.CommitAsync();
        }
    }
}

}

