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

            Console.WriteLine(bigQueryInsertRows.Sum(x => x.CalculateSize()));

            //return;

            var protoData = new AppendRowsRequest.Types.ProtoData
            {
                WriterSchema = _writerSchema,
                Rows = new ProtoRows
                {
                    SerializedRows = { bigQueryInsertRows.Select(x => x.ToByteString()) },
                },
            };

            var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(5000));
            var appendRowsStream = _bigQueryWriteClientBuilder.AppendRows(CallSettings.FromCancellationToken(cancellationTokenSource.Token));

            var appendResultsHandlerTask = Task.Run(async () =>
            {
                await foreach (var response in appendRowsStream.GetResponseStream())
                {
                    Console.WriteLine($"Appending rows resulted in: {response}");
                }
            });

            await appendRowsStream.WriteAsync(new AppendRowsRequest
            {
                ProtoRows = protoData,
                WriteStream = _writeStreamName,
            });

            await appendRowsStream.WriteCompleteAsync();
            await appendResultsHandlerTask;
        }
    }
}