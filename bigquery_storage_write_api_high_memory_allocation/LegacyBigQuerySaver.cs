using System.Collections.Concurrent;
using System.Diagnostics;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;
using Google.Protobuf;

namespace bigquery_storage_write_api_high_memory_allocation
{
    public sealed class LegacyBigQuerySaver
    {
        private bool _started;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly BigQueryTable _bigQueryTable;

        public LegacyBigQuerySaver()
        {
            //var googleCredential = GoogleCredential.FromJson(@"REDACTED");
            //const string projectId = "REDACTED";
            //const string datasetId = $"REDACTED";
            //const string tableId = "REDACTED";
            //var client = BigQueryClient.Create(projectId: projectId, credential: googleCredential);
            //_bigQueryTable = client.GetTable(datasetId: datasetId, tableId: tableId);
        }

        public void LegacyInsert(List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> list)
        {
            // capacity is not length! This is here to pre-alloc the underlying array, so that it isn't needlessly resized
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

            var bigQueryInsertRows = new List<BigQueryInsertRow>(capacity: localConcurrentDictionary.Count);

            foreach (var pair in localConcurrentDictionary)
            {
                bigQueryInsertRows.Add(new BigQueryInsertRow
                {
                    new WatchtowerBigQueryModel().ToLegacyStreamingInsertRow(pair.Key, pair.Value)
                });
            }

            try
            {
                //_bigQueryTable.InsertRows(bigQueryInsertRows);
            }
            catch (Exception)
            {
                Console.WriteLine("A general error occurred sending calculation stats to big query. Items will be re-queued");
            }
        }
    }
}