// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;

namespace MyCoreApiTest.Demo
{
    // public class SourceDbToELastic2
    // {
        
    // }
    using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using System.Threading.Tasks;

class Program
{
    private static readonly HttpClient httpClient = new();

    static async Task Main()
    {
        string connectionString = "Server=myServer;Database=myDB;User Id=myUser;Password=myPass;";
        string query = "SELECT Name, Age, DOB, Salary FROM Employees";
        string elasticsearchUrl = "http://localhost:9200/my-index/_bulk";

        var fieldMappings = new List<FieldMapping>
        {
            new("Name", "full_name", "string", "string"),
            new("Age", "age", "int", "int"),
            new("DOB", "date_of_birth", "string", "date"),
            new("Salary", "monthly_salary", "float", "double")
        };

        await StreamDbToElasticsearchAsync(connectionString, query, elasticsearchUrl, fieldMappings);
    }

    static async Task StreamDbToElasticsearchAsync(string connectionString, string query, string elasticsearchUrl, List<FieldMapping> fieldMappings)
    {
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var producerTask = Task.Run(() => ReadDbStreamAsync(connectionString, query, channel.Writer, fieldMappings));
        var consumerTask = Task.Run(() => PushToElasticsearchAsync(channel.Reader, elasticsearchUrl));

        await Task.WhenAll(producerTask, consumerTask);
    }

    static async Task ReadDbStreamAsync(string connectionString, string query, ChannelWriter<string> writer, List<FieldMapping> fieldMappings)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        using var command = new SqlCommand(query, connection);
        using var reader = await command.ExecuteReaderAsync();

        List<Task> tasks = new();

        while (await reader.ReadAsync())
        {
            var batch = new List<Dictionary<string, object>>(1000);

            for (int i = 0; i < 1000 && await reader.ReadAsync(); i++)
            {
                var jsonObject = new Dictionary<string, object>();

                foreach (var mapping in fieldMappings)
                {
                    object? value = reader[mapping.SourceField];
                    jsonObject[mapping.DestinationField] = ConvertValue(value, mapping.SourceType, mapping.DestinationType);
                }

                batch.Add(jsonObject);
            }

            var task = Task.Run(async () =>
            {
                foreach (var json in ProcessBatch(batch))
                {
                    await writer.WriteAsync(json);
                }
            });

            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
        writer.Complete();
    }

    static IEnumerable<string> ProcessBatch(List<Dictionary<string, object>> batch)
    {
        foreach (var jsonObject in batch)
        {
            var jsonPayload = new
            {
                index = new { _index = "my-index" }
            };

            yield return $"{JsonSerializer.Serialize(jsonPayload)}\n{JsonSerializer.Serialize(jsonObject)}";
        }
    }

    static object ConvertValue(object? value, string sourceType, string destinationType)
    {
        if (value == DBNull.Value) return destinationType switch
        {
            "int" or "float" or "double" => 0,
            _ => null!
        };

        return destinationType switch
        {
            "int" => Convert.ToInt32(value),
            "float" => Convert.ToSingle(value),
            "double" => Convert.ToDouble(value),
            "date" => DateTime.TryParse(value.ToString(), out var dateValue) ? dateValue.ToString("o") : null!,
            _ => value.ToString()?.Trim() ?? ""
        };
    }

    static async Task PushToElasticsearchAsync(ChannelReader<string> reader, string elasticsearchUrl)
    {
        var batchSize = 500;
        var buffer = new List<string>();

        await foreach (var json in reader.ReadAllAsync())
        {
            buffer.Add(json);
            if (buffer.Count >= batchSize)
            {
                await SendBatchToElasticsearch(buffer, elasticsearchUrl);
                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            await SendBatchToElasticsearch(buffer, elasticsearchUrl);
        }
    }

    static async Task SendBatchToElasticsearch(List<string> batch, string elasticsearchUrl)
    {
        string bulkPayload = string.Join("\n", batch) + "\n";
        var content = new StringContent(bulkPayload, Encoding.UTF8, "application/json");

        var response = await httpClient.PostAsync(elasticsearchUrl, content);
        response.EnsureSuccessStatusCode();
    }
}

public record FieldMapping(string SourceField, string DestinationField, string SourceType, string DestinationType);

}