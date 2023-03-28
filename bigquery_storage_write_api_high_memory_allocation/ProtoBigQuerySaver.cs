using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;

namespace bigquery_storage_write_api_high_memory_allocation
{
    public sealed class ProtoBigQuerySaver
    {
        private readonly string _writeStreamName;
        private readonly ProtoSchema _writerSchema;
        private readonly BigQueryWriteClient _bigQueryWriteClientBuilder;

        public ProtoBigQuerySaver(GoogleCredential googleCredential, string projectId, string datasetId, string tableId)
        {
            _writeStreamName = WriteStreamName.Format(projectId: projectId, datasetId: datasetId, tableId: tableId, streamId: "_default");

            _bigQueryWriteClientBuilder = new BigQueryWriteClientBuilder
            {
                Credential = googleCredential,
            }.Build();

            _writerSchema = new ProtoSchema
            {
                ProtoDescriptor = WatchtowerRecord.Descriptor.ToProto(),
            };
        }

        public async Task Insert(List<Tuple<Fields, Counters>> list)
        {
            var bigQueryInsertRows = new List<WatchtowerRecord>();

            foreach (var pair in list)
            {
                bigQueryInsertRows.Add(ToProtobufRow(pair.Item1, pair.Item2));
            }

            var appendRowsStream = _bigQueryWriteClientBuilder.AppendRows();

            foreach (var records in bigQueryInsertRows.Chunk(400))
            {
                var protoData = new AppendRowsRequest.Types.ProtoData
                {
                    WriterSchema = _writerSchema,
                    Rows = new ProtoRows
                    {
                        SerializedRows = { records.Select(x => x.ToByteString()) },
                    },
                };

                await appendRowsStream.WriteAsync(new AppendRowsRequest
                {
                    ProtoRows = protoData,
                    WriteStream = _writeStreamName,
                }).ConfigureAwait(false);
            }

            await appendRowsStream.WriteCompleteAsync().ConfigureAwait(false);
        }

        private static WatchtowerRecord ToProtobufRow(Fields fields, Counters counters)
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
}