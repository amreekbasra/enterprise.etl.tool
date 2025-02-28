// using System;
// using System.Collections.Concurrent;
// using System.Collections.Generic;
// using System.Linq;
// using System.Text.Json;
// using System.Threading.Tasks;
// using Microsoft.AspNetCore.Mvc;

// namespace MyCoreApiTest
// {
//     public class BreakFileInChunksAndRead
//     {
        
//     }

    using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Confluent.Kafka;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Configure Serilog for logging
builder.Host.UseSerilog((context, config) =>
{
    config.WriteTo.Console()
          .WriteTo.File("logs/log.txt")
          .WriteTo.Elasticsearch(new Serilog.Sinks.Elasticsearch.ElasticsearchSinkOptions(new Uri("http://localhost:9200"))
          {
              AutoRegisterTemplate = true,
          });
});

builder.Services.AddControllers();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddSingleton<IDataStreamService, DataStreamService>();

var app = builder.Build();

app.UseSerilogRequestLogging();
app.UseRouting();
app.UseAuthorization();
app.MapControllers();

app.Run();

// Kafka Producer Service
public interface IKafkaProducer
{
    Task PublishMessageAsync(string topic, string message);
}

public class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer()
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishMessageAsync(string topic, string message)
    {
        await _producer.ProduceAsync(topic, new Message<string, string> { Key = null, Value = message });
    }
}

// Kafka Consumer Service
// Kafka Consumer Service
public interface IKafkaConsumer
{
    void StartConsumer(string topic);
}

public class KafkaConsumer : IKafkaConsumer
{
    private readonly IDatabaseService _databaseService;
    public KafkaConsumer(IDatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    public void StartConsumer(string topic)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "data-stream-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var cancellationToken = new CancellationTokenSource();
        Parallel.ForEach(Enumerable.Range(0, Environment.ProcessorCount), _ =>
        {
            while (!cancellationToken.Token.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(cancellationToken.Token);
                    _databaseService.InsertDataAsync(result.Value).Wait();
                    consumer.Commit(result);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error consuming message: {ex.Message}");
                }
            }
        });
    }
}

// Database Service
public interface IDatabaseService
{
    Task InsertDataAsync(string jsonData);
}

public class DatabaseService : IDatabaseService
{
    private readonly string _connectionString = "User Id=your_user;Password=your_password;Data Source=your_db";

    public async Task InsertDataAsync(string jsonData)
    {
        var data = JsonSerializer.Deserialize<Dictionary<string, string>>(jsonData);
        var columns = string.Join(", ", data.Keys);
        var values = string.Join(", ", data.Values.Select(v => $"'{v}'"));
        var query = $"INSERT INTO your_table ({columns}) VALUES ({values})";

        using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();
        using var command = new OracleCommand(query, connection);
        await command.ExecuteNonQueryAsync();
    }
}

// API Controller for Data Streaming
[ApiController]
[Route("api/datastream")]
public class DataStreamController : ControllerBase
{
    private readonly IDataStreamService _dataStreamService;

    public DataStreamController(IDataStreamService dataStreamService)
    {
        _dataStreamService = dataStreamService;
    }

    [HttpPost("process")]
    public async Task<IActionResult> ProcessData([FromBody] DataStreamRequest request)
    {
        await _dataStreamService.ProcessDataAsync2(request);
        return Ok("Processing Started");
    }
}

public class DataStreamRequest
{
    public string FileName { get; set; }
    public string SourceType { get; set; } // File or DB
    public string DestinationType { get; set; } // DB or File
    public string DestinationDB { get; set; }
    public string DestinationPath { get; set; }
    public string SourcePath { get; set; }
    public string TableName { get; set; }
    public string SchemaName { get; set; }
    public List<FieldMapping> ListOfSourceFieldMappingWithDestinationFieldsWithType { get; set; }
}

public class FieldMapping
{
    public string SourceField { get; set; }
    public string DestinationField { get; set; }
    public string DataType { get; set; }
}

public interface IDataStreamService
{
    Task ProcessDataAsync2(DataStreamRequest request);
}

public class DataStreamService : IDataStreamService
{
    private readonly IKafkaProducer _kafkaProducer;
    public DataStreamService(IKafkaProducer kafkaProducer)
    {
        _kafkaProducer = kafkaProducer;
    }

    public async Task ProcessDataAsync1(DataStreamRequest request)
    {
        Console.WriteLine($"Processing data for {request.FileName}");
        var filePath = Path.Combine(request.SourcePath, request.FileName);

        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException("File not found", filePath);
        }

        var lines = File.ReadLines(filePath).ToList();
        if (lines.Count <= 1) return;

        var headers = lines.First().Split(",");
        var dataLines = lines.Skip(1).ToList();

        var options = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        var chunkSize = 5000;

        Parallel.ForEach(Partitioner.Create(0, dataLines.Count, chunkSize), options, async range =>
        {
            var chunk = dataLines.Skip(range.Item1).Take(range.Item2 - range.Item1).ToList();
            var mappedChunk = chunk.Select(line => MapFields(line, headers, request.ListOfSourceFieldMappingWithDestinationFieldsWithType));
            foreach (var message in mappedChunk)
            {
                await _kafkaProducer.PublishMessageAsync("data-stream-topic", message);
            }
        });
    }
public async Task ProcessDataAsync2(DataStreamRequest request)
    {
        Console.WriteLine($"Processing data for {request.FileName}");
        var filePath = Path.Combine(request.SourcePath, request.FileName);

        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException("File not found", filePath);
        }

        using var reader = new StreamReader(filePath);
        string? headerLine = await reader.ReadLineAsync();
        if (headerLine == null) return;

        var headers = headerLine.Split(",");
        var chunkSize = 5000;
        var chunk = new List<string>(chunkSize);

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) continue;
            
            chunk.Add(line);
            if (chunk.Count >= chunkSize)
            {
                await ProcessChunkAsync(chunk, headers, request);
                chunk.Clear();
            }
        }
        
        if (chunk.Count > 0)
        {
            await ProcessChunkAsync(chunk, headers, request);
        }
    }
    private string MapFields(string line, string[] headers, List<FieldMapping> mappings)
    {
        var values = line.Split(",");
        var mappedValues = mappings.Select(m => {
            var index = Array.IndexOf(headers, m.SourceField);
            return index >= 0 ? values[index] : "";
        });
        return string.Join(",", mappedValues);
    }
 private async Task ProcessChunkAsync(List<string> chunk, string[] headers, DataStreamRequest request)
    {
        var mappedChunk = chunk.Select(line => MapFieldsToJson(line, headers, request.ListOfSourceFieldMappingWithDestinationFieldsWithType)).ToList();
        foreach (var message in mappedChunk)
        {
            await _kafkaProducer.PublishMessageAsync("data-stream-topic", message);
        }
    }
    private string MapFieldsToJson(string line, string[] headers, List<FieldMapping> mappings)
    {
        var values = line.Split(",");
        var jsonDict = new Dictionary<string, string>();
        
        foreach (var mapping in mappings)
        {
            var index = Array.IndexOf(headers, mapping.SourceField);
            if (index >= 0 && index < values.Length)
            {
                jsonDict[mapping.DestinationField] = values[index];
            }
        }
        
        return JsonSerializer.Serialize(jsonDict);
    }
}

