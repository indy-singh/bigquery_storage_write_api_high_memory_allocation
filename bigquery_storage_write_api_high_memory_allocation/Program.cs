using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using static bigquery_storage_write_api_high_memory_allocation.WatchtowerBigQueryModel;

namespace bigquery_storage_write_api_high_memory_allocation;

public class Program
{
    public static void Main(string[] args)
    {
        AppDomain.MonitoringIsEnabled = true;
        // fix the random so we have a repeatability of data.
        var random = new Random(1987);

        var list = new List<Tuple<Fields, Counters>>();
        var now = DateTime.UtcNow;
        var dayBucket = now.Date;
        var guids = new List<Guid>();

        for (int i = 0; i < 5000; i++)
        {
            var buffer = new byte[16];
            random.NextBytes(buffer);
            guids.Add(new Guid(buffer));
        }

        for (int x = 0; x < 30_000; x++)
        {
            var fields = new Fields
            {
                MinuteBucket = dayBucket.AddMinutes((int)now.TimeOfDay.TotalMinutes + random.Next(-2 +2)), // smear this around artificially
                UserReference = guids[random.Next(0, guids.Count)].ToString(), // in reality this is a set of 1000s
                SystemId = 1, // in reality this is a set of 20
                ApplicationName = "ApplicationName", // in reality this is a set of 10
                RequestTypeName = "RequestTypeName", // in reality this is a set of 1000s
                StatusCode = 1, // 2xx, 3xx, 4xx, 5xx
                Revision = 314246, // in reality this is a set of 10-20
                HostName = Environment.MachineName, // in reality this is a set of 12
                ExternalApplicationName = "ExternalApplicationName", // in reality this is a set of ~200
                IpAddress = IPAddress.Any.ToString(), // in reality this is a set of 1000s
            };
            var counters = new Counters
            {
                TotalDuration = random.Next(1, 1000),
                TotalSquareDuration = random.Next(1, 1000),
                RequestBytes = random.Next(1, 1000),
                ResponseBytes = random.Next(1, 1000),
                PgSessions = random.Next(1, 1000),
                SqlSessions = random.Next(1, 1000),
                PgStatements = random.Next(1, 1000),
                SqlStatements = random.Next(1, 1000),
                PgEntities = random.Next(1, 1000),
                SqlEntities = random.Next(1, 1000),
                CassandraStatements = random.Next(1, 1000),
                Hits = random.Next(1, 1000),
            };
            list.Add(Tuple.Create(fields, counters));
        }


        var localConcurrentDictionary = new ConcurrentDictionary<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>();

        foreach (var item in list)
        {
            (WatchtowerBigQueryModel.Fields key, WatchtowerBigQueryModel.Counters value) = item;

            WatchtowerBigQueryModel.Counters UpdateValueFactory(WatchtowerBigQueryModel.Fields existingKey, WatchtowerBigQueryModel.Counters existingValue)
            {
                existingValue.TotalDuration += value.TotalDuration;
                existingValue.TotalSquareDuration += value.TotalSquareDuration;
                existingValue.RequestBytes += value.RequestBytes;
                existingValue.ResponseBytes += value.ResponseBytes;
                existingValue.PgSessions += value.PgSessions;
                existingValue.SqlSessions += value.SqlSessions;
                existingValue.PgStatements += value.PgStatements;
                existingValue.SqlStatements += value.SqlStatements;
                existingValue.PgEntities += value.PgEntities;
                existingValue.SqlEntities += value.SqlEntities;
                existingValue.CassandraStatements += value.CassandraStatements;
                existingValue.Hits += value.Hits;
                return existingValue;
            }

            localConcurrentDictionary.AddOrUpdate(key, value, UpdateValueFactory);
        }

        //DoLegacyInsertTest();
        //DoProtobufInsertTest();

        Console.WriteLine($"Took: {AppDomain.CurrentDomain.MonitoringTotalProcessorTime.TotalMilliseconds:#,###} ms");
        Console.WriteLine($"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
        Console.WriteLine($"Peak Working Set: {Process.GetCurrentProcess().PeakWorkingSet64 / 1024:#,#} kb");

        for (var index = 0; index <= GC.MaxGeneration; index++)
            Console.WriteLine($"Gen {index} collections: {GC.CollectionCount(index)}");

        Console.WriteLine(localConcurrentDictionary.Count); // this should be around ~5,000 to be LIVE-like
    }
}