using System.Collections.Concurrent;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;

namespace bigquery_storage_write_api_high_memory_allocation
{
    public sealed class LegacyBigQuerySaver
    {
        private readonly BigQueryTable _bigQueryTable;

        public LegacyBigQuerySaver(GoogleCredential googleCredential, string projectId, string datasetId, string tableId)
        {
            var client = BigQueryClient.Create(projectId: projectId, credential: googleCredential);
            _bigQueryTable = client.GetTable(datasetId: datasetId, tableId: tableId);
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

            var bigQueryInsertRows = new List<BigQueryInsertRow>(capacity: localConcurrentDictionary.Count);

            foreach (var pair in localConcurrentDictionary)
            {
                bigQueryInsertRows.Add(new BigQueryInsertRow
                {
                    new WatchtowerBigQueryModel().ToLegacyStreamingInsertRow(pair.Key, pair.Value)
                });
            }

            await _bigQueryTable.InsertRowsAsync(bigQueryInsertRows);
        }
    }
}