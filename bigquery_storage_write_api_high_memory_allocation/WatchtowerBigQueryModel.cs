namespace bigquery_storage_write_api_high_memory_allocation;

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