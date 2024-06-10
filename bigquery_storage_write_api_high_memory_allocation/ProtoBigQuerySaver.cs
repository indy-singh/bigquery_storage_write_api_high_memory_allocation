using Google.Api.Gax.Grpc;
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
                //GrpcAdapter = GrpcCoreAdapter.Instance
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

            using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var appendRowsStream = _bigQueryWriteClientBuilder.AppendRows(CallSettings.FromCancellationToken(cancellationTokenSource.Token));

            await appendRowsStream.WriteAsync(new AppendRowsRequest
            {
                ProtoRows = protoData,
                WriteStream = _writeStreamName,
            });

            await appendRowsStream.WriteCompleteAsync().ConfigureAwait(false);

            // this is disposable and recommended here: https://github.com/googleapis/google-cloud-dotnet/issues/10034#issuecomment-1491902289
            var responseStream = appendRowsStream.GetResponseStream();
            await using (responseStream)
            {
                // need to consume the response even if we don't do anything with it
                // other slow ass memory leak: https://github.com/googleapis/google-cloud-dotnet/issues/10034#issuecomment-1491449182
                await foreach (var appendRowsResponse in responseStream)
                {

                }
            }
        }
    }
}