//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Linq;
//using System.Threading;
//using System.Threading.Tasks;
//using Codeweavers.ConfigManager;
//using Codeweavers.Logging;
//using Codeweavers.Communication;
//using Google.Apis.Auth.OAuth2;
//using Google.Cloud.BigQuery.V2;
//using Newtonsoft.Json;
//using Google.Cloud.BigQuery.Storage.V1;
//using Google.Protobuf;
//using static Google.Cloud.BigQuery.Storage.V1.BigQueryWriteClient;
//using Microsoft.Extensions.Logging;

//namespace DataMining.Data.Watchtower
//{
//    public sealed class WatchTowerBigQuerySaver : IStartable
//    {
//        private bool _started;
//        private static readonly Lazy<WatchTowerBigQuerySaver> Lazy = new Lazy<WatchTowerBigQuerySaver>(() => new WatchTowerBigQuerySaver());
//        public static WatchTowerBigQuerySaver Instance => Lazy.Value;
//        private static readonly ILogger Log = Logger.GetLogger<WatchTowerBigQuerySaver>();
//        private readonly ThirdPartyLogger _thirdPartyLogger = Logger.GetThirdPartyLogger(typeof(WatchTowerBigQuerySaver).FullName);
//        private CancellationTokenSource _cancellationTokenSource;
//        private readonly BigQueryTable _bigQueryTable;
//        private readonly ConcurrentQueue<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> _concurrentQueue;
//        private DateTime _lastSent;
//        private Task _sendToBigQueryTask;
//        private readonly string _writeStreamName;
//        private readonly AppendRowsStream _appendRowsStream;
//        private readonly ProtoSchema _writerSchema;
//        private const int MAX_BATCH_SIZE = 10_000;
//        private const int MAX_BACK_PRESSURE = 40_000;

//        private WatchTowerBigQuerySaver()
//        {
//            _started = false;
//            _cancellationTokenSource = new CancellationTokenSource();
//            _concurrentQueue = new ConcurrentQueue<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>>();
//            _lastSent = DateTime.UtcNow;

//            try
//            {
//                var googleCredential = GoogleCredential.FromJson(@"REDACTED");

//                var environment = Settings.Config.Environment.ToUpper();
//                const string projectId = "REDACTED";
//                var datasetId = $"{environment}_REDACTED";
//                const string tableId = "REDACTED";

//                _writeStreamName = WriteStreamName.Format(projectId: projectId, datasetId: datasetId, tableId: tableId, streamId: "_default");
//                var client = BigQueryClient.Create(projectId: projectId, credential: googleCredential);
//                _bigQueryTable = client.GetTable(datasetId: datasetId, tableId: tableId);

//                var bigQueryWriteClientBuilder = new BigQueryWriteClientBuilder
//                {
//                    Credential = googleCredential,
//                }.Build();

//                _appendRowsStream = bigQueryWriteClientBuilder.AppendRows();

//                _writerSchema = new ProtoSchema
//                {
//                    ProtoDescriptor = WatchtowerRecord.Descriptor.ToProto(),
//                };
//            }
//            catch (Exception exception)
//            {
//                Log.Error(new LogDetails("Failed to connect to Big Query. This means no stats will be saved to BigQuery.")
//                {
//                    Exception = exception
//                });
//            }
//        }

//        private async Task SendToBigQuery(CancellationToken cancellationToken)
//        {
//            // There are a few important knobs here:-
//            // === OtherBigQueryStatsSaverPulseTimeInMs ===
//            //  This is how often the while loop will run, and the conditions are checked.
//            //  The closer to zero the more CPU you will eat, the higher the number, the more chance you have of creating a memory leak.
//            // 
//            // === OtherBigQueryStatsSaverLastSentInMs ===
//            //  This applies to condition b (note the or nature of the if check) we want to sent as soon as we have a decent amount of rows to send
//            //  The closer to zero the request to big query you will incur (700ms on average for the max batch size of 10,000), the higher the number, the more chance you have of creating a memory leak.
//            while (cancellationToken.IsCancellationRequested == false)
//            {
//                // we have at minimum the max number of rows that big query can batch insert
//                var conditionA = _concurrentQueue.Count >= MAX_BATCH_SIZE;
//                // or it has been over x timeunit (e.g. 10 seconds) since we last sent and we have at least one row to send
//                var conditionB = _lastSent.Add(TimeSpan.FromMilliseconds(Settings.Config.IntFrom("OtherBigQueryStatsSaverLastSentInMs"))) < DateTime.UtcNow && _concurrentQueue.Count > 0;

//                if (conditionA || conditionB)
//                {
//                    if (Settings.Config.IsEnabled("FT_UseBigqueryStorageWriteAPI"))
//                    {
//                        await StorageWriteApi();
//                    }
//                    else
//                    {
//                        await ExpensiveStreamingInsert();
//                    }
//                }
//                else
//                {
//                    await Task.Delay(TimeSpan.FromMilliseconds(Settings.Config.IntFrom("OtherBigQueryStatsSaverPulseTimeInMs")), cancellationToken);
//                }
//            }
//        }

//        private async Task StorageWriteApi()
//        {
//            // max batch insert is 10,000.
//            var length = Math.Min(MAX_BATCH_SIZE, _concurrentQueue.Count);

//            // capacity is not length! This is here to pre-alloc the underlying array, so that it isn't needlessly resized
//            var backup = new List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>>(capacity: length);
//            var localConcurrentDictionary = new ConcurrentDictionary<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>();
//            var list = new List<WatchtowerRecord>(capacity: length);

//            for (int i = 0; i < length; i++)
//            {
//                if (_concurrentQueue.TryDequeue(out var item))
//                {
//                    (WatchtowerBigQueryModel.Fields key, WatchtowerBigQueryModel.Counters value) = item;

//                    WatchtowerBigQueryModel.Counters UpdateValueFactory(WatchtowerBigQueryModel.Fields existingKey, WatchtowerBigQueryModel.Counters existingValue)
//                    {
//                        existingValue.TotalDuration += value.TotalDuration;
//                        existingValue.TotalSquareDuration += value.TotalSquareDuration;
//                        existingValue.RequestBytes += value.RequestBytes;
//                        existingValue.ResponseBytes += value.ResponseBytes;
//                        existingValue.PgSessions += value.PgSessions;
//                        existingValue.SqlSessions += value.SqlSessions;
//                        existingValue.PgStatements += value.PgStatements;
//                        existingValue.SqlStatements += value.SqlStatements;
//                        existingValue.PgEntities += value.PgEntities;
//                        existingValue.SqlEntities += value.SqlEntities;
//                        existingValue.CassandraStatements += value.CassandraStatements;
//                        existingValue.Hits += value.Hits;
//                        return existingValue;
//                    }

//                    localConcurrentDictionary.AddOrUpdate(key, value, UpdateValueFactory);

//                    backup.Add(item);
//                }
//            }

//            foreach (var pair in localConcurrentDictionary)
//            {
//                list.Add(new WatchtowerBigQueryModel().ToStorageWriteApi(pair.Key, pair.Value));
//            }

//            _lastSent = DateTime.UtcNow;

//            var dropThreshold = Math.Min(MAX_BACK_PRESSURE, Settings.Config.IntFrom("OtherBigQueryStatsSaverDropThreshold"));

//            // dropping the remaining queue over the max back pressure size is important to maintain the health of the cluster (e.g. memory & cpu)
//            // It takes around on average 647.9176121302088856 ms (n of 9431) to send a full batch of 10,000 rows to big query
//            // the largest dropped remaining queue we've seen is 89,611 which occurred at 2021-03-03 00:56:00.042608+00
//            // calculations are very bursty in load, we seem to do a consistent load and then have a massive spike for a short few seconds
//            // so to begin with I'm setting the max back pressure to 40,000, this is AFTER we have taken the maximum possible from the queue
//            // so in effect, the entire limit is 50,000
//            if (_concurrentQueue.Count > dropThreshold)
//            {
//                // instead of dropping the entire queue, we now only drop the number beyond the threshold.
//                // previously if the max back pressure we set to 40,000, and the back pressure had reached 41,234 we would have dropped all 41,234 items
//                // now we only drop the items past that threshold, so 1,234
//                var numberOfItemsToDequeue = _concurrentQueue.Count - dropThreshold;

//                _thirdPartyLogger.Error(new LogDetails($"Dropping {numberOfItemsToDequeue}")
//                {
//                    ObjectDump = new
//                    {
//                        total = _concurrentQueue.Count,
//                        dropThreshold = dropThreshold,
//                        numberOfItemsToDequeue = numberOfItemsToDequeue
//                    }
//                });

//                for (var i = 0; i < numberOfItemsToDequeue; i++)
//                {
//                    _concurrentQueue.TryDequeue(out _);
//                }
//            }

//            // Only send if the config is enabled and we managed to connect to the BigQuery
//            // The above happens regardless to prevent a memory leak
//            if (Settings.Config.IsEnabled("UseBigQueryForWatchtower") && _appendRowsStream is object)
//            {
//                var sw = Stopwatch.StartNew();

//                try
//                {
//                    // this does not use the start-stop cancellation token because we want to give big query a maximum of 5 seconds to batch insert
//                    var protoData = new AppendRowsRequest.Types.ProtoData
//                    {
//                        WriterSchema = _writerSchema,
//                        Rows = new ProtoRows
//                        {
//                            // https://github.com/protocolbuffers/protobuf/issues/12217 -> ToByteString memory leak
//                            SerializedRows = { list.Select(x => x.ToByteString()) },
//                        },
//                    };

//                    await _appendRowsStream.WriteAsync(new AppendRowsRequest
//                    {
//                        ProtoRows = protoData,
//                        WriteStream = _writeStreamName,
//                    });

//                    if (Settings.Config.IsEnabled("OtherBigQueryStatsSaverLogsOn"))
//                    {
//                        Log.Warn(new LogDetails($"Aggregated {length} individual saves to be {localConcurrentDictionary.Count} saves")
//                        {
//                            ObjectDump = new
//                            {
//                                count = list.Count,
//                                duration = sw.Elapsed.TotalMilliseconds,
//                                remaining = _concurrentQueue.Count,
//                                beforeAggr = length,
//                                afterAggr = localConcurrentDictionary.Count,
//                                sizeOfRequest = list.Sum(x => x.CalculateSize())
//                            }
//                        });
//                    }
//                }
//                catch (TaskCanceledException exception)
//                {
//                    _thirdPartyLogger.Error(new LogDetails("It took longer than five seconds to send calculation stats to big query. Items will be re-queued")
//                    {
//                        ObjectDump = new
//                        {
//                            count = list.Count,
//                            duration = sw.Elapsed.TotalMilliseconds,
//                            remaining = _concurrentQueue.Count,
//                            beforeAggr = length,
//                            afterAggr = localConcurrentDictionary.Count,
//                        },
//                        Exception = exception
//                    });

//                    ReQueue(backup);
//                }
//                catch (Exception exception)
//                {
//                    _thirdPartyLogger.Error(new LogDetails("A general error occurred sending calculation stats to big query. Items will be re-queued")
//                    {
//                        ObjectDump = new
//                        {
//                            count = list.Count,
//                            duration = sw.Elapsed.TotalMilliseconds,
//                            remaining = _concurrentQueue.Count,
//                            beforeAggr = length,
//                            afterAggr = localConcurrentDictionary.Count,
//                        },
//                        Exception = exception
//                    });

//                    ReQueue(backup);
//                }
//            }
//        }

//        private async Task ExpensiveStreamingInsert()
//        {
//            // max batch insert is 10,000.
//            var length = Math.Min(MAX_BATCH_SIZE, _concurrentQueue.Count);

//            // capacity is not length! This is here to pre-alloc the underlying array, so that it isn't needlessly resized
//            var backup = new List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>>(capacity: length);
//            var localConcurrentDictionary = new ConcurrentDictionary<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>();
//            var list = new List<BigQueryInsertRow>(capacity: length);

//            for (int i = 0; i < length; i++)
//            {
//                if (_concurrentQueue.TryDequeue(out var item))
//                {
//                    (WatchtowerBigQueryModel.Fields key, WatchtowerBigQueryModel.Counters value) = item;

//                    WatchtowerBigQueryModel.Counters UpdateValueFactory(WatchtowerBigQueryModel.Fields existingKey, WatchtowerBigQueryModel.Counters existingValue)
//                    {
//                        existingValue.TotalDuration += value.TotalDuration;
//                        existingValue.TotalSquareDuration += value.TotalSquareDuration;
//                        existingValue.RequestBytes += value.RequestBytes;
//                        existingValue.ResponseBytes += value.ResponseBytes;
//                        existingValue.PgSessions += value.PgSessions;
//                        existingValue.SqlSessions += value.SqlSessions;
//                        existingValue.PgStatements += value.PgStatements;
//                        existingValue.SqlStatements += value.SqlStatements;
//                        existingValue.PgEntities += value.PgEntities;
//                        existingValue.SqlEntities += value.SqlEntities;
//                        existingValue.CassandraStatements += value.CassandraStatements;
//                        existingValue.Hits += value.Hits;
//                        return existingValue;
//                    }

//                    localConcurrentDictionary.AddOrUpdate(key, value, UpdateValueFactory);

//                    backup.Add(item);
//                }
//            }

//            foreach (var pair in localConcurrentDictionary)
//            {
//                list.Add(new BigQueryInsertRow
//                {
//                    new WatchtowerBigQueryModel().ToInsertRowApi(pair.Key, pair.Value)
//                });
//            }

//            _lastSent = DateTime.UtcNow;

//            var dropThreshold = Math.Min(MAX_BACK_PRESSURE, Settings.Config.IntFrom("OtherBigQueryStatsSaverDropThreshold"));

//            // dropping the remaining queue over the max back pressure size is important to maintain the health of the cluster (e.g. memory & cpu)
//            // It takes around on average 647.9176121302088856 ms (n of 9431) to send a full batch of 10,000 rows to big query
//            // the largest dropped remaining queue we've seen is 89,611 which occurred at 2021-03-03 00:56:00.042608+00
//            // calculations are very bursty in load, we seem to do a consistent load and then have a massive spike for a short few seconds
//            // so to begin with I'm setting the max back pressure to 40,000, this is AFTER we have taken the maximum possible from the queue
//            // so in effect, the entire limit is 50,000
//            if (_concurrentQueue.Count > dropThreshold)
//            {
//                // instead of dropping the entire queue, we now only drop the number beyond the threshold.
//                // previously if the max back pressure we set to 40,000, and the back pressure had reached 41,234 we would have dropped all 41,234 items
//                // now we only drop the items past that threshold, so 1,234
//                var numberOfItemsToDequeue = _concurrentQueue.Count - dropThreshold;

//                _thirdPartyLogger.Error(new LogDetails($"Dropping {numberOfItemsToDequeue}")
//                {
//                    ObjectDump = new
//                    {
//                        total = _concurrentQueue.Count,
//                        dropThreshold = dropThreshold,
//                        numberOfItemsToDequeue = numberOfItemsToDequeue
//                    }
//                });

//                for (var i = 0; i < numberOfItemsToDequeue; i++)
//                {
//                    _concurrentQueue.TryDequeue(out _);
//                }
//            }

//            // Only send if the config is enabled and we managed to connect to the BigQuery
//            // The above happens regardless to prevent a memory leak
//            if (Settings.Config.IsEnabled("UseBigQueryForWatchtower") && _bigQueryTable is object)
//            {
//                var sw = Stopwatch.StartNew();

//                try
//                {
//                    // this does not use the start-stop cancellation token because we want to give big query a maximum of 5 seconds to batch insert
//                    await _bigQueryTable.InsertRowsAsync(list, null, new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

//                    if (Settings.Config.IsEnabled("OtherBigQueryStatsSaverLogsOn"))
//                    {
//                        Log.Warn(new LogDetails($"Aggregated {length} individual saves to be {localConcurrentDictionary.Count} saves")
//                        {
//                            ObjectDump = new
//                            {
//                                count = list.Count,
//                                duration = sw.Elapsed.TotalMilliseconds,
//                                remaining = _concurrentQueue.Count,
//                                beforeAggr = length,
//                                afterAggr = localConcurrentDictionary.Count,
//                            }
//                        });
//                    }
//                }
//                catch (TaskCanceledException exception)
//                {
//                    _thirdPartyLogger.Error(new LogDetails("It took longer than five seconds to send calculation stats to big query. Items will be re-queued")
//                    {
//                        ObjectDump = new
//                        {
//                            count = list.Count,
//                            duration = sw.Elapsed.TotalMilliseconds,
//                            remaining = _concurrentQueue.Count,
//                            beforeAggr = length,
//                            afterAggr = localConcurrentDictionary.Count,
//                        },
//                        Exception = exception
//                    });

//                    ReQueue(backup);
//                }
//                catch (Exception exception)
//                {
//                    _thirdPartyLogger.Error(new LogDetails("A general error occurred sending calculation stats to big query. Items will be re-queued")
//                    {
//                        ObjectDump = new
//                        {
//                            count = list.Count,
//                            duration = sw.Elapsed.TotalMilliseconds,
//                            remaining = _concurrentQueue.Count,
//                            beforeAggr = length,
//                            afterAggr = localConcurrentDictionary.Count,
//                        },
//                        Exception = exception
//                    });

//                    ReQueue(backup);
//                }
//            }
//        }

//        /// <summary>
//        /// We only want to requeue when we get an exception around sending items to be saved to BigQuery.
//        /// This is safe to do because if the back pressure gets too much, then the drop threshold will kick in
//        /// </summary>
//        /// <param name="list"></param>
//        private void ReQueue(List<Tuple<WatchtowerBigQueryModel.Fields, WatchtowerBigQueryModel.Counters>> list)
//        {
//            var before = _concurrentQueue.Count;
//            foreach (var item in list)
//            {
//                _concurrentQueue.Enqueue(item);
//            }

//            var after = _concurrentQueue.Count;
//            var toBeRequeued = list.Count;

//            Log.Warn(new LogDetails("ReQueue was called")
//            {
//                ObjectDump = new
//                {
//                    before = before,
//                    after = after,
//                    toBeRequeued = toBeRequeued
//                }
//            });
//        }

//        public void Start()
//        {
//            if (_started is false)
//            {
//                Log.Warn(new LogDetails("Starting BigQueryStatsSaver!"));
//                _cancellationTokenSource = new CancellationTokenSource();
//                _sendToBigQueryTask = Task.Run(() => SendToBigQuery(_cancellationTokenSource.Token), _cancellationTokenSource.Token);
//                _started = true;
//                Log.Warn(new LogDetails("Started BigQueryStatsSaver!"));
//            }
//            else
//            {
//                Log.Warn(new LogDetails("BigQueryStatsSaver is already started!"));
//            }
//        }

//        public void Stop()
//        {
//            if (Settings.Config.IsEnabled("UseBigQueryForWatchtower"))
//            {
//                if (_started is true)
//                {
//                    Log.Warn(new LogDetails("Attempting to stop BigQueryStatsSaver!"));
//                    _cancellationTokenSource.Cancel();
//                    var isCanceled = _sendToBigQueryTask.IsCanceled;
//                    var isCompleted = _sendToBigQueryTask.IsCompleted;
//                    var isFaulted = _sendToBigQueryTask.IsFaulted;
//                    _sendToBigQueryTask = Task.CompletedTask;
//                    _started = false;
//                    Log.Warn(new LogDetails("Stopped BigQueryStatsSaver!")
//                    {
//                        ObjectDump = new
//                        {
//                            isCanceled,
//                            isCompleted,
//                            isFaulted,
//                        }
//                    });
//                }
//                else
//                {
//                    Log.Warn(new LogDetails("BigQueryStatsSaver is already stopped!"));
//                }
//            }
//        }

//        public void Enqueue(WatchtowerBigQueryModel.Fields fields, WatchtowerBigQueryModel.Counters counters)
//        {
//            // Only add to queue if the app is started AND the config is enabled and we managed to connect to the BigQuery
//            // Last condition is important, otherwise memory leak!
//            if (_started is true && Settings.Config.IsEnabled("UseBigQueryForWatchtower") && _bigQueryTable is object)
//            {
//                _concurrentQueue.Enqueue(Tuple.Create(fields, counters));
//            }
//        }

//        public sealed class WatchtowerBigQueryModel
//        {
//            public class Fields
//            {
//                public string UserReference { get; set; }
//                public long SystemId { get; set; }
//                public DateTime MinuteBucket { get; set; }
//                public string ApplicationName { get; set; }
//                public string RequestTypeName { get; set; }
//                public long StatusCode { get; set; }
//                public long Revision { get; set; }
//                public string HostName { get; set; }
//                public string ExternalApplicationName { get; set; }
//                public string IpAddress { get; set; }

//                protected bool Equals(Fields other)
//                {
//                    return UserReference == other.UserReference && SystemId == other.SystemId && MinuteBucket.Equals(other.MinuteBucket) && ApplicationName == other.ApplicationName && RequestTypeName == other.RequestTypeName && StatusCode == other.StatusCode && Revision == other.Revision && HostName == other.HostName && ExternalApplicationName == other.ExternalApplicationName && IpAddress == other.IpAddress;
//                }

//                public override bool Equals(object obj)
//                {
//                    if (ReferenceEquals(null, obj)) return false;
//                    if (ReferenceEquals(this, obj)) return true;
//                    if (obj.GetType() != this.GetType()) return false;
//                    return Equals((Fields)obj);
//                }

//                public override int GetHashCode()
//                {
//                    var hashCode = new HashCode();
//                    hashCode.Add(UserReference);
//                    hashCode.Add(SystemId);
//                    hashCode.Add(MinuteBucket);
//                    hashCode.Add(ApplicationName);
//                    hashCode.Add(RequestTypeName);
//                    hashCode.Add(StatusCode);
//                    hashCode.Add(Revision);
//                    hashCode.Add(HostName);
//                    hashCode.Add(ExternalApplicationName);
//                    hashCode.Add(IpAddress);
//                    return hashCode.ToHashCode();
//                }
//            }

//            public class Counters
//            {
//                public long TotalDuration { get; set; }
//                public long TotalSquareDuration { get; set; }
//                public long RequestBytes { get; set; }
//                public long ResponseBytes { get; set; }
//                public long PgSessions { get; set; }
//                public long SqlSessions { get; set; }
//                public long PgStatements { get; set; }
//                public long SqlStatements { get; set; }
//                public long CassandraStatements { get; set; }
//                public long PgEntities { get; set; }
//                public long SqlEntities { get; set; }
//                public long Hits { get; set; }
//            }

//            public Dictionary<string, object> ToInsertRowApi(Fields fields, Counters counters)
//            {
//                return CreateRow(fields, counters);
//            }

//            public string ToUploadJsonApi(Fields fields, Counters counters)
//            {
//                var mappedResponse = CreateRow(fields, counters);
//                return JsonConvert.SerializeObject(mappedResponse, Formatting.None);
//            }

//            public WatchtowerRecord ToStorageWriteApi(Fields fields, Counters counters)
//            {
//                var storageWriteApi = new WatchtowerRecord
//                {
//                    MinuteBucket = new DateTimeOffset(fields.MinuteBucket).ToUnixTimeMilliseconds() * 1000,
//                    UserReference = fields.UserReference,
//                    SystemId = fields.SystemId,
//                    ApplicationName = fields.ApplicationName,
//                    RequestTypeName = fields.RequestTypeName,
//                    StatusCode = fields.StatusCode,
//                    Revision = fields.Revision,
//                    HostName = fields.HostName,
//                    ExternalApplicationName = fields.ExternalApplicationName,
//                    IpAddress = fields.IpAddress,

//                    TotalDuration = counters.TotalDuration,
//                    TotalSquareDuration = counters.TotalSquareDuration,
//                    RequestBytes = counters.RequestBytes,
//                    ResponseBytes = counters.ResponseBytes,
//                    PgSessions = counters.PgSessions,
//                    SqlSessions = counters.SqlSessions,
//                    PgStatements = counters.PgStatements,
//                    SqlStatements = counters.SqlStatements,
//                    PgEntities = counters.PgEntities,
//                    SqlEntities = counters.SqlEntities,
//                    CassandraStatements = counters.CassandraStatements,
//                    Hits = counters.Hits,
//                };

//                if (Settings.Config.IsEnabled("OtherBigQueryStatsSaverLogsOn"))
//                {
//                    var calculateSize = storageWriteApi.CalculateSize();

//                    Log.Warn(new LogDetails("Protobuf size")
//                    {
//                        ObjectDump = new
//                        {
//                            calculateSize
//                        }
//                    });
//                }

//                return storageWriteApi;
//            }

//            private static Dictionary<string, object> CreateRow(Fields fields, Counters counters)
//            {
//                return new Dictionary<string, object>
//                {
//                    ["minute_bucket"] = fields.MinuteBucket,
//                    ["user_reference"] = fields.UserReference,
//                    ["system_id"] = fields.SystemId,
//                    ["application_name"] = fields.ApplicationName,
//                    ["request_type_name"] = fields.RequestTypeName,
//                    ["status_code"] = fields.StatusCode,
//                    ["revision"] = fields.Revision,
//                    ["host_name"] = fields.HostName,
//                    ["external_application_name"] = fields.ExternalApplicationName,
//                    ["ip_address"] = fields.IpAddress,

//                    ["total_duration"] = counters.TotalDuration,
//                    ["total_square_duration"] = counters.TotalSquareDuration,
//                    ["request_bytes"] = counters.RequestBytes,
//                    ["response_bytes"] = counters.ResponseBytes,
//                    ["pg_sessions"] = counters.PgSessions,
//                    ["sql_sessions"] = counters.SqlSessions,
//                    ["pg_statements"] = counters.PgStatements,
//                    ["sql_statements"] = counters.SqlStatements,
//                    ["pg_entities"] = counters.PgEntities,
//                    ["sql_entities"] = counters.SqlEntities,
//                    ["cassandra_statements"] = counters.CassandraStatements,
//                    ["hits"] = counters.Hits,
//                };
//            }
//        }
//    }
//}