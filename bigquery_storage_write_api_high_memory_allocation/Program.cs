﻿using Google.Apis.Auth.OAuth2;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace bigquery_storage_write_api_high_memory_allocation;

public class Program
{
    public static async Task Main(string[] args)
    {
        AppDomain.MonitoringIsEnabled = true;

        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        var googleCredential = GoogleCredential.FromFile(@"creds.json");
        var projectId = config.GetSection("projectId").Value;
        var datasetId = config.GetSection("datasetId").Value;
        var tableId = config.GetSection("tableId").Value;

        var tuples = DummyData.Get();
        var legacyBigQuerySaver = new LegacyBigQuerySaver(googleCredential, projectId, datasetId, tableId);
        var protoBigQuerySaver = new ProtoBigQuerySaver(googleCredential, projectId, datasetId, tableId);

        for (var index = 0; index < tuples.Count; index++)
        {
            var tuple = tuples[index];
            await protoBigQuerySaver.Insert(tuple);
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            Console.WriteLine($"{index:D4}/{tuples.Count}\t{AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
        }

        Console.WriteLine($"Took: {AppDomain.CurrentDomain.MonitoringTotalProcessorTime.TotalMilliseconds:#,###} ms");
        Console.WriteLine($"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
        Console.WriteLine($"Peak Working Set: {Process.GetCurrentProcess().PeakWorkingSet64 / 1024:#,#} kb");

        for (var index = 0; index <= GC.MaxGeneration; index++)
            Console.WriteLine($"Gen {index} collections: {GC.CollectionCount(index)}");
    }
}