using System.Collections.Concurrent;
using System.Reflection;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using static Google.Cloud.BigQuery.Storage.V1.BigQueryWriteClient;

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

        public async Task Insert(Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters> item)
        {
            var protoData = new AppendRowsRequest.Types.ProtoData
            {
                WriterSchema = _writerSchema,
                Rows = new ProtoRows
                {
                    SerializedRows = { new WatchtowerBigQueryModel().ToProtobufRow(item.Item1, item.Item2).ToByteString() },
                },
            };

            var appendRowsStream = _bigQueryWriteClientBuilder.AppendRows();

            DoThing(appendRowsStream, "a");

            await appendRowsStream.WriteAsync(new AppendRowsRequest
            {
                ProtoRows = protoData,
                WriteStream = _writeStreamName,
            });

            DoThing(appendRowsStream, "b");

            await appendRowsStream.WriteCompleteAsync();

            DoThing(appendRowsStream, "c");
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

            DoThing(appendRowsStream, "a");

            await appendRowsStream.WriteAsync(new AppendRowsRequest
            {
                ProtoRows = protoData,
                WriteStream = _writeStreamName,
            });

            DoThing(appendRowsStream, "b");

            await appendRowsStream.WriteCompleteAsync();

            DoThing(appendRowsStream, "c");
        }

        private static void DoThing(AppendRowsStream appendRowsStream, string tag)
        {
            //var fieldInfo = appendRowsStream.GetType().GetField("_writeBuffer", BindingFlags.Instance | BindingFlags.NonPublic);

            //if (fieldInfo is object)
            //{
            //    var writeBuffer = fieldInfo.GetValue(appendRowsStream);

            //    if (writeBuffer is object)
            //    {
            //        var propertyInfo = writeBuffer.GetType().GetProperty("BufferedWriteCount", BindingFlags.Instance | BindingFlags.NonPublic);

            //        if (propertyInfo is object)
            //        {
            //            var bufferedWriteCount = propertyInfo.GetValue(writeBuffer);

            //            Console.WriteLine(string.Join("\t", tag, bufferedWriteCount));
            //        }
            //    }
            //}
        }
    }
}