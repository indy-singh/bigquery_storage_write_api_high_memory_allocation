using System.Collections.Concurrent;
using System.Diagnostics;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using static Google.Cloud.BigQuery.Storage.V1.BigQueryWriteClient;

namespace bigquery_storage_write_api_high_memory_allocation
{
    public sealed class ProtoBigQuerySaver
    {
        private bool _started;
        private static readonly Lazy<ProtoBigQuerySaver> Lazy = new Lazy<ProtoBigQuerySaver>(() => new ProtoBigQuerySaver());
        public static ProtoBigQuerySaver Instance => Lazy.Value;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly ConcurrentQueue<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> _concurrentQueue;
        private DateTime _lastSent;
        private Task _sendToBigQueryTask;
        private readonly string _writeStreamName;
        private readonly AppendRowsStream _appendRowsStream;
        private readonly ProtoSchema _writerSchema;
        private const int MAX_BATCH_SIZE = 10_000;
        private const int MAX_BACK_PRESSURE = 40_000;

        private ProtoBigQuerySaver()
        {
            _started = false;
            _cancellationTokenSource = new CancellationTokenSource();
            _concurrentQueue = new ConcurrentQueue<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>>();
            _lastSent = DateTime.UtcNow;

            try
            {
                var googleCredential = GoogleCredential.FromJson(@"REDACTED");

                const string projectId = "REDACTED";
                var datasetId = $"REDACTED";
                const string tableId = "REDACTED";

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
            catch (Exception exception)
            {
                Console.WriteLine(exception);
            }
        }

        private async Task SendToBigQuery(CancellationToken cancellationToken)
        {
            // There are a few important knobs here:-
            // === OtherBigQueryStatsSaverPulseTimeInMs ===
            //  This is how often the while loop will run, and the conditions are checked.
            //  The closer to zero the more CPU you will eat, the higher the number, the more chance you have of creating a memory leak.
            // 
            // === OtherBigQueryStatsSaverLastSentInMs ===
            //  This applies to condition b (note the or nature of the if check) we want to sent as soon as we have a decent amount of rows to send
            //  The closer to zero the request to big query you will incur (700ms on average for the max batch size of 10,000), the higher the number, the more chance you have of creating a memory leak.
            while (cancellationToken.IsCancellationRequested == false)
            {
                // we have at minimum the max number of rows that big query can batch insert
                var conditionA = _concurrentQueue.Count >= MAX_BATCH_SIZE;
                // or it has been over x timeunit (e.g. 10 seconds) since we last sent and we have at least one row to send
                var conditionB = _lastSent.Add(TimeSpan.FromMilliseconds(20000)) < DateTime.UtcNow && _concurrentQueue.Count > 0;

                if (conditionA || conditionB)
                {
                    await StorageWriteApi();
                }
                else
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                }
            }
        }

        private async Task StorageWriteApi()
        {
            // max batch insert is 10,000.
            var length = Math.Min(MAX_BATCH_SIZE, _concurrentQueue.Count);

            // capacity is not length! This is here to pre-alloc the underlying array, so that it isn't needlessly resized
            var backup = new List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>>(capacity: length);
            var localConcurrentDictionary = new ConcurrentDictionary<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>();
            var list = new List<WatchtowerRecord>(capacity: length);

            for (int i = 0; i < length; i++)
            {
                if (_concurrentQueue.TryDequeue(out var item))
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

                    backup.Add(item);
                }
            }

            foreach (var pair in localConcurrentDictionary)
            {
                list.Add(new WatchtowerBigQueryModel().ToProtobufRow(pair.Key, pair.Value));
            }

            _lastSent = DateTime.UtcNow;

            var dropThreshold = Math.Min(MAX_BACK_PRESSURE, 40000);

            // dropping the remaining queue over the max back pressure size is important to maintain the health of the cluster (e.g. memory & cpu)
            // It takes around on average 647.9176121302088856 ms (n of 9431) to send a full batch of 10,000 rows to big query
            // the largest dropped remaining queue we've seen is 89,611 which occurred at 2021-03-03 00:56:00.042608+00
            // calculations are very bursty in load, we seem to do a consistent load and then have a massive spike for a short few seconds
            // so to begin with I'm setting the max back pressure to 40,000, this is AFTER we have taken the maximum possible from the queue
            // so in effect, the entire limit is 50,000
            if (_concurrentQueue.Count > dropThreshold)
            {
                // instead of dropping the entire queue, we now only drop the number beyond the threshold.
                // previously if the max back pressure we set to 40,000, and the back pressure had reached 41,234 we would have dropped all 41,234 items
                // now we only drop the items past that threshold, so 1,234
                var numberOfItemsToDequeue = _concurrentQueue.Count - dropThreshold;

                Console.WriteLine($"Dropping {numberOfItemsToDequeue}");

                for (var i = 0; i < numberOfItemsToDequeue; i++)
                {
                    _concurrentQueue.TryDequeue(out _);
                }
            }

            // Only send if the config is enabled and we managed to connect to the BigQuery
            // The above happens regardless to prevent a memory leak
            if (_appendRowsStream is object)
            {
                var sw = Stopwatch.StartNew();

                try
                {
                    // this does not use the start-stop cancellation token because we want to give big query a maximum of 5 seconds to batch insert
                    var protoData = new AppendRowsRequest.Types.ProtoData
                    {
                        WriterSchema = _writerSchema,
                        Rows = new ProtoRows
                        {
                            // https://github.com/protocolbuffers/protobuf/issues/12217 -> ToByteString memory leak
                            SerializedRows = { list.Select(x => x.ToByteString()) },
                        },
                    };

                    await _appendRowsStream.WriteAsync(new AppendRowsRequest
                    {
                        ProtoRows = protoData,
                        WriteStream = _writeStreamName,
                    });

                    Console.WriteLine($"Aggregated {length} individual saves to be {localConcurrentDictionary.Count} saves");
                }
                catch (TaskCanceledException exception)
                {
                    Console.WriteLine("It took longer than five seconds to send calculation stats to big query. Items will be re-queued");

                    ReQueue(backup);
                }
                catch (Exception exception)
                {
                    Console.WriteLine("A general error occurred sending calculation stats to big query. Items will be re-queued");
                    ReQueue(backup);
                }
            }
        }

        /// <summary>
        /// We only want to requeue when we get an exception around sending items to be saved to BigQuery.
        /// This is safe to do because if the back pressure gets too much, then the drop threshold will kick in
        /// </summary>
        /// <param name="list"></param>
        private void ReQueue(List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> list)
        {
            foreach (var item in list)
            {
                _concurrentQueue.Enqueue(item);
            }

            Console.WriteLine("ReQueue was called");
        }

        public void Start()
        {
            if (_started is false)
            {
                Console.WriteLine("Starting BigQueryStatsSaver!");
                _cancellationTokenSource = new CancellationTokenSource();
                _sendToBigQueryTask = Task.Run(() => SendToBigQuery(_cancellationTokenSource.Token), _cancellationTokenSource.Token);
                _started = true;
                Console.WriteLine("Started BigQueryStatsSaver!");
            }
            else
            {
                Console.WriteLine("BigQueryStatsSaver is already started!");
            }
        }

        public void Stop()
        {
            if (_started is true)
            {
                Console.WriteLine("Attempting to stop BigQueryStatsSaver!");
                _cancellationTokenSource.Cancel();
                _sendToBigQueryTask = Task.CompletedTask;
                _started = false;
                Console.WriteLine("Stopped BigQueryStatsSaver!");
            }
            else
            {
            }
        }

        public void Enqueue(WatchtowerBigQueryModel.Fields fields, WatchtowerBigQueryModel.Counters counters)
        {
            // Only add to queue if the app is started AND the config is enabled and we managed to connect to the BigQuery
            // Last condition is important, otherwise memory leak!
            if (_started is true)
            {
                _concurrentQueue.Enqueue(Tuple.Create(fields, counters));
            }
        }
    }
}