using System.Collections.Concurrent;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using static Google.Cloud.BigQuery.Storage.V1.BigQueryWriteClient;

namespace bigquery_storage_write_api_high_memory_allocation
{
    public sealed class ProtoBigQuerySaver
    {
        private readonly string _writeStreamName;
        private readonly AppendRowsStream _appendRowsStream;
        private readonly ProtoSchema _writerSchema;

        public ProtoBigQuerySaver(GoogleCredential googleCredential, string projectId, string datasetId, string tableId)
        {
            _writeStreamName = WriteStreamName.Format(projectId: projectId, datasetId: datasetId, tableId: tableId, streamId: "_default");

            var bigQueryWriteClientBuilder = new BigQueryWriteClientBuilder
            {
                Credential = googleCredential,
            }.Build();

            _appendRowsStream = bigQueryWriteClientBuilder.AppendRows();

            _writerSchema = new ProtoSchema
            {
                ProtoDescriptor = WatchtowerRecord.Descriptor.ToProto(),
            };
        }

        public async Task Insert(List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> list)
        {
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

            var bigQueryInsertRows = new List<WatchtowerRecord>(capacity: localConcurrentDictionary.Count);

            foreach (var pair in localConcurrentDictionary)
            {
                bigQueryInsertRows.Add(new WatchtowerBigQueryModel().ToProtobufRow(pair.Key, pair.Value));
            }

            var protoData = new AppendRowsRequest.Types.ProtoData
            {
                WriterSchema = _writerSchema,
                Rows = new ProtoRows
                {
                    // https://github.com/protocolbuffers/protobuf/issues/12217 -> ToByteString memory leak
                    SerializedRows = { bigQueryInsertRows.Select(x => x.ToByteString()) },
                },
            };

            await _appendRowsStream.WriteAsync(new AppendRowsRequest
            {
                ProtoRows = protoData,
                WriteStream = _writeStreamName,
            });
        }
    }
}