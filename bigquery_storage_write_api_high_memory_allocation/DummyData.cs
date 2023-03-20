using System.Net;

namespace bigquery_storage_write_api_high_memory_allocation;

public static class DummyData
{
    public static List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> Get(string needle)
    {
        var random = new Random(1987);

        var list = new List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>>();
        var now = DateTime.UtcNow;
        var dayBucket = now.Date;

        for (int x = 0; x < 5_000; x++)
        {
            var buffer = new byte[16];
            random.NextBytes(buffer);

            var fields = new WatchtowerBigQueryModel.Fields
            {
                MinuteBucket = dayBucket.AddMinutes((int)now.TimeOfDay.TotalMinutes + random.Next(-2 + 2)), // smear this around artificially
                UserReference = new Guid(buffer).ToString(), // in reality this is a set of 1000s
                SystemId = 1, // in reality this is a set of 20
                ApplicationName = "ApplicationName", // in reality this is a set of 10
                RequestTypeName = "RequestTypeName", // in reality this is a set of 1000s
                StatusCode = 1, // 2xx, 3xx, 4xx, 5xx
                Revision = 314246, // in reality this is a set of 10-20
                HostName = needle, // in reality this is a set of 12
                ExternalApplicationName = "ExternalApplicationName", // in reality this is a set of ~200
                IpAddress = IPAddress.Any.ToString(), // in reality this is a set of 1000s
            };
            var counters = new WatchtowerBigQueryModel.Counters
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

        return list;
    }
}