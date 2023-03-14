namespace bigquery_storage_write_api_high_memory_allocation;

public sealed class WatchtowerBigQueryModel
{
    public class Fields
    {
        public string UserReference { get; set; }
        public long SystemId { get; set; }
        public DateTime MinuteBucket { get; set; }
        public string ApplicationName { get; set; }
        public string RequestTypeName { get; set; }
        public long StatusCode { get; set; }
        public long Revision { get; set; }
        public string HostName { get; set; }
        public string ExternalApplicationName { get; set; }
        public string IpAddress { get; set; }

        protected bool Equals(Fields other)
        {
            return UserReference == other.UserReference && SystemId == other.SystemId && MinuteBucket.Equals(other.MinuteBucket) && ApplicationName == other.ApplicationName && RequestTypeName == other.RequestTypeName && StatusCode == other.StatusCode && Revision == other.Revision && HostName == other.HostName && ExternalApplicationName == other.ExternalApplicationName && IpAddress == other.IpAddress;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Fields)obj);
        }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(UserReference);
            hashCode.Add(SystemId);
            hashCode.Add(MinuteBucket);
            hashCode.Add(ApplicationName);
            hashCode.Add(RequestTypeName);
            hashCode.Add(StatusCode);
            hashCode.Add(Revision);
            hashCode.Add(HostName);
            hashCode.Add(ExternalApplicationName);
            hashCode.Add(IpAddress);
            return hashCode.ToHashCode();
        }
    }

    public class Counters
    {
        public long TotalDuration { get; set; }
        public long TotalSquareDuration { get; set; }
        public long RequestBytes { get; set; }
        public long ResponseBytes { get; set; }
        public long PgSessions { get; set; }
        public long SqlSessions { get; set; }
        public long PgStatements { get; set; }
        public long SqlStatements { get; set; }
        public long CassandraStatements { get; set; }
        public long PgEntities { get; set; }
        public long SqlEntities { get; set; }
        public long Hits { get; set; }
    }

    public Dictionary<string, object> ToLegacyStreamingInsertRow(Fields fields, Counters counters)
    {
        return new Dictionary<string, object>
        {
            ["minute_bucket"] = fields.MinuteBucket,
            ["user_reference"] = fields.UserReference,
            ["system_id"] = fields.SystemId,
            ["application_name"] = fields.ApplicationName,
            ["request_type_name"] = fields.RequestTypeName,
            ["status_code"] = fields.StatusCode,
            ["revision"] = fields.Revision,
            ["host_name"] = fields.HostName,
            ["external_application_name"] = fields.ExternalApplicationName,
            ["ip_address"] = fields.IpAddress,

            ["total_duration"] = counters.TotalDuration,
            ["total_square_duration"] = counters.TotalSquareDuration,
            ["request_bytes"] = counters.RequestBytes,
            ["response_bytes"] = counters.ResponseBytes,
            ["pg_sessions"] = counters.PgSessions,
            ["sql_sessions"] = counters.SqlSessions,
            ["pg_statements"] = counters.PgStatements,
            ["sql_statements"] = counters.SqlStatements,
            ["pg_entities"] = counters.PgEntities,
            ["sql_entities"] = counters.SqlEntities,
            ["cassandra_statements"] = counters.CassandraStatements,
            ["hits"] = counters.Hits,
        };
    }

    public WatchtowerRecord ToProtobufRow(Fields fields, Counters counters)
    {
        return new WatchtowerRecord
        {
            // this hack is because proto requires it in microseconds.
            // https://cloud.google.com/bigquery/docs/write-api#data_type_conversions
            // NET6 doesn't have ToUnixMicroseconds, but NET/78+ does.
            MinuteBucket = new DateTimeOffset(fields.MinuteBucket).ToUnixTimeMilliseconds() * 1000,
            UserReference = fields.UserReference,
            SystemId = fields.SystemId,
            ApplicationName = fields.ApplicationName,
            RequestTypeName = fields.RequestTypeName,
            StatusCode = fields.StatusCode,
            Revision = fields.Revision,
            HostName = fields.HostName,
            ExternalApplicationName = fields.ExternalApplicationName,
            IpAddress = fields.IpAddress,

            TotalDuration = counters.TotalDuration,
            TotalSquareDuration = counters.TotalSquareDuration,
            RequestBytes = counters.RequestBytes,
            ResponseBytes = counters.ResponseBytes,
            PgSessions = counters.PgSessions,
            SqlSessions = counters.SqlSessions,
            PgStatements = counters.PgStatements,
            SqlStatements = counters.SqlStatements,
            PgEntities = counters.PgEntities,
            SqlEntities = counters.SqlEntities,
            CassandraStatements = counters.CassandraStatements,
            Hits = counters.Hits,
        };
    }
}