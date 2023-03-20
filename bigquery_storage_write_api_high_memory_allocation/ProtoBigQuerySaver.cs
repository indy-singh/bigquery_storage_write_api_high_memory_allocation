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

        public async Task Insert(List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> list)
        {
            var bigQueryInsertRows = new List<WatchtowerRecord>();

            foreach (var pair in list)
            {
                bigQueryInsertRows.Add(new WatchtowerBigQueryModel().ToProtobufRow(pair.Item1, pair.Item2));
            }

            var protoData = new AppendRowsRequest.Types.ProtoData
            {
                WriterSchema = _writerSchema,
                Rows = new ProtoRows
                {
                    SerializedRows = { bigQueryInsertRows.Select(x => x.ToByteString()) },
                },
            };

            var appendRowsStream = _bigQueryWriteClientBuilder.AppendRows();

            await appendRowsStream.WriteAsync(new AppendRowsRequest
            {
                ProtoRows = protoData,
                WriteStream = _writeStreamName,
            });

            await appendRowsStream.WriteCompleteAsync();
        }

        public async Task InsertInChunks(List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> list)
        {
            var watchtowerBigQueryModel = new WatchtowerBigQueryModel();

            var appendRowsStream = _bigQueryWriteClientBuilder.AppendRows();

            // we pick chunks of 400 because that keeps well below the 81,920 threshold
            // https://referencesource.microsoft.com/#mscorlib/system/io/stream.cs,53
            foreach (var chunk in list.Chunk(400))
            {
                var watchtowerRecords = chunk.Select(x => watchtowerBigQueryModel.ToProtobufRow(x.Item1, x.Item2));

                var protoData = new AppendRowsRequest.Types.ProtoData
                {
                    WriterSchema = _writerSchema,
                    Rows = new ProtoRows
                    {
                        SerializedRows = { watchtowerRecords.Select(x => x.ToByteString()) },
                    },
                };

                await appendRowsStream.WriteAsync(new AppendRowsRequest
                {
                    ProtoRows = protoData,
                    WriteStream = _writeStreamName,
                });
            }

            await appendRowsStream.WriteCompleteAsync();
        }
    }
}