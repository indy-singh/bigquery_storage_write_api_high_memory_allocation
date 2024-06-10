using Google.Apis.Auth.OAuth2;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using Microsoft.Extensions.Configuration;

namespace bigquery_storage_write_api_high_memory_allocation;

public class Program
{
    public static async Task Main(string[] args)
    {
        AppDomain.MonitoringIsEnabled = true;

        //_ = new MyListener();

        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var googleCredential = GoogleCredential.FromFile(@"creds.json");
        var projectId = config.GetSection("projectId").Value;
        var datasetId = config.GetSection("datasetId").Value;
        var tableId = config.GetSection("tableId").Value;

        var tuples = DummyData.Get();
        var protoBigQuerySaver = new ProtoBigQuerySaver(googleCredential, projectId, datasetId, tableId);
        var loop = 100;

        for (var index = 0; index < loop; index++)
        {
            try
            {
                await protoBigQuerySaver.Insert(tuples);
                var format = string.Join("\t", $"SUCCESS:{index:D5}/{loop:D5}", DateTimeOffset.UtcNow.ToString("O"), $"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
                Console.WriteLine(format);
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
            catch (Exception exception)
            {
                var format = string.Join("\t", $"FAILURE:{index:D5}/{loop:D5}", DateTimeOffset.UtcNow.ToString("O"), $"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb", exception.ToString());
                Console.WriteLine(format);
            }
        }

        Console.WriteLine($"Took: {AppDomain.CurrentDomain.MonitoringTotalProcessorTime.TotalMilliseconds:#,###} ms");
        Console.WriteLine($"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
        Console.WriteLine($"Peak Working Set: {Process.GetCurrentProcess().PeakWorkingSet64 / 1024:#,#} kb");

        for (var index = 0; index <= GC.MaxGeneration; index++)
            Console.WriteLine($"Gen {index} collections: {GC.CollectionCount(index)}");
    }
}

public sealed class MyListener : EventListener
{
    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if ("Private.InternalDiagnostics.System.Net.Quic".Equals(eventSource.Name))
        {
            EnableEvents(eventSource, EventLevel.LogAlways, EventKeywords.All);
        }
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        Console.WriteLine($"{DateTime.UtcNow:ss:fff} {eventData.EventName}: " +
                          string.Join(' ', eventData.PayloadNames!.Zip(eventData.Payload!).Select(pair => $"{pair.First}={pair.Second}")));

    }
}