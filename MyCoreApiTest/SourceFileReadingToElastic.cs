// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;

// namespace MyCoreApiTest
// {
//     public class SourceFileReadingToElastic
//     {
        
//     }
// }

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
        string inputFilePath = "largefile.csv";
        string elasticsearchUrl = "http://localhost:9200/my-index/_bulk"; // Change to your index URL

        // Define field mapping: (sourceField, destinationField, sourceDataType, destinationDataType)
        var fieldMappings = new List<FieldMapping>
        {
            new("Name", "full_name", "string", "string"),
            new("Age", "age", "int", "int"),
            new("DOB", "date_of_birth", "string", "date"),
            new("Salary", "monthly_salary", "float", "double")
        };

        await ProcessLargeFileAsync(inputFilePath, elasticsearchUrl, fieldMappings);
    }

    static async Task ProcessLargeFileAsync(string inputFile, string elasticsearchUrl, List<FieldMapping> fieldMappings)
    {
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var producerTask = Task.Run(() => ReadFileAsync(inputFile, channel.Writer, fieldMappings));
        var consumerTask = Task.Run(() => PushToElasticsearchAsync(channel.Reader, elasticsearchUrl));

        await Task.WhenAll(producerTask, consumerTask);
    }

    static async Task ReadFileAsync(string filePath, ChannelWriter<string> writer, List<FieldMapping> fieldMappings)
    {
        using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
        using var reader = new StreamReader(fs);

        string? headerLine = await reader.ReadLineAsync();
        if (headerLine is null) throw new InvalidOperationException("File is empty");

        string[] headers = headerLine.Split(',');

        List<Task> tasks = new();

        while (!reader.EndOfStream)
        {
            var batch = new List<string>(1000);

            for (int i = 0; i < 1000 && !reader.EndOfStream; i++)
            {
                string? line = await reader.ReadLineAsync();
                if (line is not null) batch.Add(line);
            }

            var task = Task.Run(async () =>
            {
                foreach (var json in ProcessBatch(batch, headers, fieldMappings))
                {
                    await writer.WriteAsync(json);
                }
            });

            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
        writer.Complete();
    }

    static IEnumerable<string> ProcessBatch(List<string> batch, string[] headers, List<FieldMapping> fieldMappings)
    {
        foreach (var line in batch)
        {
            var values = line.Split(',');
            var jsonObject = new Dictionary<string, object>();

            foreach (var mapping in fieldMappings)
            {
                int sourceIndex = Array.IndexOf(headers, mapping.SourceField);
                if (sourceIndex == -1 || sourceIndex >= values.Length) continue;

                jsonObject[mapping.DestinationField] = ConvertValue(values[sourceIndex], mapping.SourceType, mapping.DestinationType);
            }

            var jsonPayload = new
            {
                index = new { _index = "my-index" }
            };

            yield return $"{JsonSerializer.Serialize(jsonPayload)}\n{JsonSerializer.Serialize(jsonObject)}";
        }
    }

    static object ConvertValue(string value, string sourceType, string destinationType)
    {
        try
        {
            return destinationType switch
            {
                "int" => int.TryParse(value, out var intValue) ? intValue : 0,
                "float" => float.TryParse(value, out var floatValue) ? floatValue : 0f,
                "double" => double.TryParse(value, out var doubleValue) ? doubleValue : 0.0,
                "date" => DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue) ? dateValue.ToString("o") : null,
                _ => value.Trim() // Default to string
            };
        }
        catch
        {
            return destinationType == "int" || destinationType == "float" || destinationType == "double" ? 0 : null!;
        }
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
        string bulkPayload = string.Join("\n", batch) + "\n"; // Bulk format requires newline termination
        var content = new StringContent(bulkPayload, Encoding.UTF8, "application/json");

        var response = await httpClient.PostAsync(elasticsearchUrl, content);
        response.EnsureSuccessStatusCode(); // Ensure it was indexed properly
    }
}

public record FieldMapping(string SourceField, string DestinationField, string SourceType, string DestinationType);

// Use a mapping list that specifies:
// sourceField (column name in the file)
// destinationField (target JSON field name)
// sourceDataType
// destinationDataType
// Apply data transformations according to the required types.
// Ensure only mapped fields appear in the JSON output.