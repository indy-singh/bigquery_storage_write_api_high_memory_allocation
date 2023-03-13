using System.Diagnostics;
using System.Net;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;
using Google.Protobuf;

namespace bigquery_storage_write_api_high_memory_allocation;

public class Program
{
    public static void Main(string[] args)
    {
        AppDomain.MonitoringIsEnabled = true;

        //DoLegacyInsertTest();
        DoStorageWriteApiTest();

        Console.WriteLine($"Took: {AppDomain.CurrentDomain.MonitoringTotalProcessorTime.TotalMilliseconds:#,###} ms");
        Console.WriteLine($"Allocated: {AppDomain.CurrentDomain.MonitoringTotalAllocatedMemorySize / 1024:#,#} kb");
        Console.WriteLine($"Peak Working Set: {Process.GetCurrentProcess().PeakWorkingSet64 / 1024:#,#} kb");

        for (var index = 0; index <= GC.MaxGeneration; index++)
            Console.WriteLine($"Gen {index} collections: {GC.CollectionCount(index)}");
    }

    private static void DoLegacyInsertTest()
    {
        var records = CreateLegacyInsertList();

        for (int i = 0; i < 1_000_000; i++)
        {
            var bigQueryInsertRows = records.Select(x => new BigQueryInsertRow{ x });
        }
    }

    private static void DoStorageWriteApiTest()
    {
        var records = CreateStorageWriteApiList();

        for (int i = 0; i < 1_000_000; i++)
        {
            var protoData = new AppendRowsRequest.Types.ProtoData
            {
                WriterSchema = new ProtoSchema
                {
                    ProtoDescriptor = WatchtowerRecord.Descriptor.ToProto()
                },
                Rows = new ProtoRows
                {
                    SerializedRows = { records.Select(x => x.ToByteString()) }
                }
            };
        }
    }

    private static List<WatchtowerRecord> CreateStorageWriteApiList()
    {
        var random = Random.Shared;
        var records = new List<WatchtowerRecord>();

        for (int i = 0; i < 3; i++)
        {
            records.Add(new WatchtowerRecord
            {
                // fields
                MinuteBucket = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + random.Next(1000, 9000)) * 1000,
                UserReference = Guid.NewGuid().ToString(),
                SystemId = random.Next(1, 10),
                ApplicationName = "ApplicationName",
                RequestTypeName = "RequestTypeName",
                StatusCode = random.Next(101, 999),
                Revision = random.Next(100000, 999999),
                HostName = Environment.MachineName,
                ExternalApplicationName = "ExternalApplicationName",
                IpAddress = IPAddress.Any.ToString(),
                // counters
                TotalDuration = random.Next(1000, 9000),
                TotalSquareDuration = random.Next(1000, 9000),
                RequestBytes = random.Next(1000, 9000),
                ResponseBytes = random.Next(1000, 9000),
                PgSessions = random.Next(1000, 9000),
                SqlSessions = random.Next(1000, 9000),
                PgStatements = random.Next(1000, 9000),
                SqlStatements = random.Next(1000, 9000),
                PgEntities = random.Next(1000, 9000),
                SqlEntities = random.Next(1000, 9000),
                CassandraStatements = random.Next(1000, 9000),
                Hits = random.Next(1, 100),
            });
        }

        return records;
    }

    private static List<Dictionary<string, object>> CreateLegacyInsertList()
    {
        var random = Random.Shared;
        var list = new List<Dictionary<string, object>>();

        for (int i = 0; i < 3; i++)
        {
            list.Add(new Dictionary<string, object>
            {
                //fields
                ["minute_bucket"] = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + random.Next(1000, 9000)) * 1000,
                ["user_reference"] = Guid.NewGuid().ToString(),
                ["system_id"] = random.Next(1, 10),
                ["application_name"] = "ApplicationName",
                ["request_type_name"] = "RequestTypeName",
                ["status_code"] = random.Next(101, 999),
                ["revision"] = random.Next(100000, 999999),
                ["host_name"] = Environment.MachineName,
                ["external_application_name"] = "ExternalApplicationName",
                ["ip_address"] = IPAddress.Any.ToString(),
                //counters
                ["total_duration"] = random.Next(1000, 9000),
                ["total_square_duration"] = random.Next(1000, 9000),
                ["request_bytes"] = random.Next(1000, 9000),
                ["response_bytes"] = random.Next(1000, 9000),
                ["pg_sessions"] = random.Next(1000, 9000),
                ["sql_sessions"] = random.Next(1000, 9000),
                ["pg_statements"] = random.Next(1000, 9000),
                ["sql_statements"] = random.Next(1000, 9000),
                ["pg_entities"] = random.Next(1000, 9000),
                ["sql_entities"] = random.Next(1000, 9000),
                ["cassandra_statements"] = random.Next(1000, 9000),
                ["hits"] = random.Next(1, 100),
            });
        }

        return list;
    }
}