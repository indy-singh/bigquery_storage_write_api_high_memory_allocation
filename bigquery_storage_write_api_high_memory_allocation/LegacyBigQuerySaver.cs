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
            var bigQueryInsertRows = new List<BigQueryInsertRow>(capacity: list.Count);

            foreach (var pair in list)
            {
                bigQueryInsertRows.Add(new BigQueryInsertRow
                {
                    new WatchtowerBigQueryModel().ToLegacyStreamingInsertRow(pair.Item1, pair.Item2)
                });
            }

            await _bigQueryTable.InsertRowsAsync(bigQueryInsertRows);
        }
    }
}