using Couchbase;
using Couchbase.Extensions.Caching;
using Couchbase.Extensions.DependencyInjection;
using Couchbase.KeyValue;
using Couchbase.Query;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Couchbase.BulkReadAndWrite
{
    public class BulkOperationsService : BackgroundService
    {
        private readonly ILogger<BulkOperationsService> _logger;
        private readonly IDistributedCache _cache;
        ICouchbaseCacheBucketProvider _couchbaseCacheBucketProvider;
        private IConfiguration _configuration;
        private readonly IClusterProvider _clusterProvider;

        public BulkOperationsService(ILogger<BulkOperationsService> logger, IDistributedCache cache,
            ICouchbaseCacheBucketProvider couchbaseCacheBucketProvider, IConfiguration configuration, IClusterProvider clusterProvider)
        {
            _logger = logger;
            _cache = cache;
            _couchbaseCacheBucketProvider = couchbaseCacheBucketProvider;
            _configuration = configuration;
            _clusterProvider = clusterProvider;
        }

        /// <summary>
        /// Parallel execution
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        //protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        //{
        //    var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * 1000;
        //    JObject jObject;
        //    //var jsonPath= @"C: \Users\subasish\OneDrive - Optym\Documents\Optym\Projects\Test Data\MaintenanceDomain.json";
        //    var jsonPath = @"MaintenanceDomain.json";
        //    using (StreamReader file = File.OpenText(jsonPath))
        //    {
        //        using (JsonTextReader reader = new JsonTextReader(file))
        //        {
        //            jObject = (JObject)JToken.ReadFrom(reader);
        //        }

        //    }
        //    var source = Enumerable.Range(cacheUpperValue - 999, cacheUpperValue).ToArray();
        //    // Partition the entire source array.
        //    var rangePartitioner = Partitioner.Create(0, source.Length);
        //    var stopWatch = new Stopwatch();
        //    stopWatch.Start();
        //    Parallel.ForEach(rangePartitioner, (range, loopState) =>
        //    {
        //        for (int i = range.Item1; i < range.Item2; i++)
        //        {
        //            try
        //            {
        //                _cache.SetString(source[i].ToString(), jObject.ToString());
        //            }
        //            catch (Exception ex)
        //            {
        //                Console.WriteLine(ex.ToString()); ;
        //            }
        //        }
        //    });
        //    await Task.Delay(2);
        //    stopWatch.Stop();
        //    Console.WriteLine($"Time elapsed in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");
        //}

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await WriteAsync();
            //await GetAsync();
            //await UpdateAsync();
            //await DeleteAsync();
            //await GetDataByPaginationAsync();
            //await GetDataByJoinAsync();         

        }

        private async Task WriteAsync()
        {
            var maxRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * maxRecords;
            JObject jObject;
            var jsonPath = @"MaintenanceDomain.json";
            using (StreamReader file = File.OpenText(jsonPath))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    jObject = (JObject)JToken.ReadFrom(reader);
                    //jObject.Add("type", "maintenance");
                }
            }
            var source = Enumerable.Range(cacheUpperValue - (maxRecords - 1), maxRecords).ToArray();
            var bucket = await _couchbaseCacheBucketProvider.GetBucketAsync();
            var scope = await bucket.ScopeAsync("cache");
            var defaultCollection = scope.Collection("maintenance");
            //var defaultCollection = await bucket.DefaultCollectionAsync();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await source.ParallelForEachAsync(
                async item =>
                {
                    try
                    {
                        //var jObj = jObject.ToObject<JObject>();
                        //jObj.Add("docId", item);
                        //var result = await defaultCollection.InsertAsync<JObject>(item.ToString(), jObj);
                        try
                        {
                            var result = await defaultCollection.InsertAsync<JObject>(item.ToString(), jObject);
                        }
                        catch (Exception ex)
                        {

                        }

                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex.ToString()); ;
                    }
                },
                Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            );
            stopWatch.Stop();
            _logger.LogWarning($"Time elapsed to insert {source.Length} records in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");
        }

        private async Task GetAsync()
        {
            var batchSize = _configuration.GetValue<int>("CacheKey:BatchSize");
            var offset = _configuration.GetValue<int>("CacheKey:Offset");
            var totalRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            if (totalRecords > batchSize)
            {
                await GetJsonRecordsInBatchesAsync(batchSize, offset, totalRecords);
                return;
            }
            await GetXNoOfJSonRecordsAsync(offset, totalRecords);
        }

        private async Task GetXNoOfJSonRecordsAsync(int offset, int totalRecords)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var filterQuery = "SELECT d.* FROM `Cache-Sample`.cache.maintenance as d where d.`type`= 'maintenance' OFFSET $offset LIMIT $limit";
                stopWatch.Start();
                var queryResult = await cluster.QueryAsync<dynamic>(filterQuery, options => options.Metrics(true)
                                                                                        .Parameter("$offset", offset)
                                                                                        .Parameter("$limit", totalRecords));
                if (queryResult.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(queryResult.Errors);
                }
                stopWatch.Stop();
                await queryResult.CountAsync();
                var recordsRead = queryResult.MetaData.Metrics.ResultCount;
                _logger.LogInformation($"Time elapsed to read {recordsRead} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {

            }
        }

        private async Task GetJsonRecordsInBatchesAsync(int batchSize, int offset, int totalRecords)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            //var queryResult = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance as d where d.`type`= 'maintenance'");
            //if (queryResult.MetaData.Status != QueryStatus.Success)
            //{
            //    ProcessError(queryResult.Errors);
            //}
            //var totalRecords = (int)(await queryResult.FirstAsync()).totalRecords;
            var noOfBatches = (totalRecords / batchSize) + (totalRecords % batchSize == 0 ? 0 : 1);
            var source = Enumerable.Range(1, noOfBatches).ToArray();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            long totalRecordsRead = 0;
            var query = "SELECT d.* FROM `Cache-Sample`.cache.maintenance as d where d.`type`= 'maintenance' OFFSET $offset LIMIT $limit";
            await source.ParallelForEachAsync(
                    async item =>
                    {
                        try
                        {
                            var queryResult = await cluster.QueryAsync<dynamic>(query, options => options.Metrics(true)
                                                                                        .Parameter("$offset", offset + ((item - 1) * batchSize))
                                                                                        .Parameter("$limit", batchSize));
                            if (queryResult.MetaData.Status != QueryStatus.Success)
                            {
                                ProcessError(queryResult.Errors);
                            }
                            await queryResult.CountAsync();
                            var recordsRead = queryResult.MetaData.Metrics.ResultCount;
                            _logger.LogInformation($"Read {recordsRead} records in {queryResult.MetaData.Metrics.ExecutionTime}");
                            Interlocked.Add(ref totalRecordsRead, (long)recordsRead);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex.ToString());
                        }

                    },
                Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            );
            stopWatch.Stop();
            _logger.LogInformation($"Total time elapsed to read {Interlocked.Read(ref totalRecordsRead)} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
        }

        private async Task UpdateAsync()
        {
            var batchSize = _configuration.GetValue<int>("CacheKey:BatchSize");
            var offset = _configuration.GetValue<int>("CacheKey:Offset");
            var totalRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            if (totalRecords > batchSize)
            {
                await UpdateJsonRecordsInBatchesAsync(batchSize, offset, totalRecords);
                return;
            }
            await UpdateXNoJsonRecordsAsync(offset, totalRecords);
        }

        private async Task UpdateJsonRecordsInBatchesAsync(int batchSize, int offset, int totalRecords)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var noOfBatches = (totalRecords / batchSize) + (totalRecords % batchSize == 0 ? 0 : 1);
            var source = Enumerable.Range(1, noOfBatches).ToArray();
            var query = "SELECT META(d).id as id FROM `Cache-Sample`.cache.maintenance as d where d.`type`= 'maintenance' OFFSET $offset LIMIT $limit";
            var bucket = await _couchbaseCacheBucketProvider.GetBucketAsync();
            var scope = await bucket.ScopeAsync("cache");
            var defaultCollection = scope.Collection("maintenance");
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await source.ParallelForEachAsync(
                  async item =>
                  {
                      try
                      {
                          //Fetch doc Id's for the batch range
                          var queryResult = await cluster.QueryAsync<dynamic>(query, options => options.Metrics(true)
                                                                                      .Parameter("$offset", offset + ((item - 1) * batchSize))
                                                                                      .Parameter("$limit", batchSize));
                          if (queryResult.MetaData.Status != QueryStatus.Success)
                          {
                              ProcessError(queryResult.Errors);
                          }
                          await foreach (var row in queryResult.Rows)
                          {
                              var docId = (string)row.id;
                              await defaultCollection.MutateInAsync(docId, specs =>
                              {
                                  specs.Upsert("createdOn", DateTime.UtcNow);
                              }
                            );
                          }
                      }
                      catch (Exception ex)
                      {
                          _logger.LogError(ex.ToString());
                      }
                  },
                Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            );
            stopWatch.Stop();
            _logger.LogWarning($"Time elapsed to update {totalRecords} records in {stopWatch.Elapsed.TotalMilliseconds} miliseconds");
        }

        private async Task UpdateXNoJsonRecordsAsync(int offset, int totalRecords)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await _couchbaseCacheBucketProvider.GetBucketAsync();
            var query = "SELECT META(d).id as id FROM `Cache-Sample`.cache.maintenance as d where d.`type`= 'maintenance' OFFSET $offset LIMIT $limit";
            var queryResult = await cluster.QueryAsync<dynamic>(query, options => options.Metrics(true)
                                                                                    .Parameter("$offset", offset)
                                                                                    .Parameter("$limit", totalRecords));
            if (queryResult.MetaData.Status != QueryStatus.Success)
            {
                ProcessError(queryResult.Errors);
            }
            var scope = await bucket.ScopeAsync("cache");
            var defaultCollection = scope.Collection("maintenance");
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await queryResult.Rows.ToEnumerable<dynamic>().ParallelForEachAsync(
                  async row =>
                  {
                      try
                      {
                          var docId = (string)row.id;
                          await defaultCollection.MutateInAsync(docId, specs =>
                          {
                              specs.Upsert("createdOn", DateTime.UtcNow);
                          }
                        );
                      }
                      catch (Exception ex)
                      {
                          _logger.LogError(ex.ToString());
                      }
                  },
                Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            );
            stopWatch.Stop();
            _logger.LogWarning($"Time elapsed to update {totalRecords} records in {stopWatch.Elapsed.TotalMilliseconds} miliseconds");
        }

        private async Task DeleteAsync()
        {
            var batchSize = _configuration.GetValue<int>("CacheKey:BatchSize");
            var offset = _configuration.GetValue<int>("CacheKey:Offset");
            var totalRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            if (totalRecords > batchSize)
            {
                await DeleteRecordsInBatchesAsync(batchSize, offset, totalRecords);
                return;
            }
            await DeleteXNoOfJSonRecords(offset, totalRecords);
        }

        private async Task DeleteRecordsInBatchesAsync(int batchSize, int offset, int totalRecords)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            try
            {
                //var batchSize = _configuration.GetValue<int>("CacheKey:BatchSize");
                //var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance WHERE d.type = 'maintenance'");
                //if (result.MetaData.Status != QueryStatus.Success)
                //{
                //    ProcessError(result.Errors);
                //}
                //var totalRecords = (int)(await result.FirstAsync()).totalRecords;
                var noOfBatches = (totalRecords / batchSize) + (totalRecords % batchSize == 0 ? 0 : 1);
                var source = Enumerable.Range(1, noOfBatches).ToArray();

                var stopWatch = new Stopwatch();
                stopWatch.Start();
                await source.ParallelForEachAsync(
                    async item =>
                    {
                        try
                        {
                            var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type and TO_NUMBER(META(d).id) BETWEEN $lowerLimit and $upperLimit";
                            //var filterQuery = "UPDATE `Cache-Sample`.cache.maintenance AS d SET META(d).expiration = 2 WHERE d.type = $type and META().id BETWEEN $lower and $upper LIMIT $batchSize";
                            //var filterQuery = "UPDATE `Cache-Sample`.cache.maintenance AS d SET META(d).expiration = 2 WHERE d.type = $type and TO_NUMBER(META(d).id) BETWEEN $lowerLimit and $upperLimit";
                            await cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                                                                                        .Parameter("$lowerLimit", offset + ((item - 1) * batchSize) + 1)
                                                                                        .Parameter("$upperLimit", item * batchSize));
                            //.Parameter("$batchSize", batchSize));
                            //await Task.Delay(100);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex.ToString());
                        }
                    },
                    Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
                );
                stopWatch.Stop();
                _logger.LogInformation($"Time elapsed to delete {totalRecords} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {
            }
        }

        private async Task DeleteXNoOfJSonRecords(int offset, int totalRecords)
        {
            var stopWatch = new Stopwatch();
            var cluster = await _clusterProvider.GetClusterAsync();
            try
            {
                var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type and TO_NUMBER(META(d).id) BETWEEN $lowerLimit and $upperLimit";
                stopWatch.Start();
                await cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                                                                                        .Parameter("$lowerLimit", offset)
                                                                                        .Parameter("$upperLimit", totalRecords));
                stopWatch.Stop();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
            stopWatch.Stop();
            _logger.LogInformation($"Time elapsed to delete {totalRecords} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
        }

        private async Task GetDataByPaginationAsync()
        {
            try
            {
                var cluster = await _clusterProvider.GetClusterAsync();
                _logger.LogInformation("Connection successful");

                var stopWatch = new Stopwatch();
                var limit = _configuration.GetValue<int>("CacheKey:PageSize");
                var offset = _configuration.GetValue<int>("CacheKey:Offset");
                var totalRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
                //var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance where `type`=$type", options =>
                //                                                                                                             options.Parameter("$type", "maintenance"));
                //if (result.MetaData.Status != QueryStatus.Success)
                //{
                //    ProcessError(result.Errors);
                //}
                //var totalRecords = (int)(await result.FirstAsync()).totalRecords;
                var actualRecordsRead = 0;
                var isError = false;
                stopWatch.Start();
                var iterations = 0;
                long pageExecutionTime = 0;
                //var filterQuery = "Select d.* from `Cache-Sample`.cache.maintenance as d where d.type=$type ORDER BY TO_NUMBER(META(d).id) OFFSET $offset LIMIT $limit";
                var filterQuery = "Select d.* from `Cache-Sample`.cache.maintenance as d where d.`type`='maintenance' OFFSET $offset LIMIT $limit";
                while (actualRecordsRead < totalRecords && iterations < 5)
                {
                    var retryAttempts = 5;
                    iterations++;
                    while (retryAttempts-- > 0)
                    {
                        try
                        {
                            var queryResult = await cluster.QueryAsync<dynamic>(filterQuery, options => options.Metrics(true)
                        .Parameter("$offset", offset)
                        .Parameter("$limit", limit)
                        );
                            if (queryResult.MetaData.Status != QueryStatus.Success)
                            {
                                ProcessError(queryResult.Errors);
                                isError = true;
                                _logger.LogInformation($"Retrying...{5 - retryAttempts}");
                            }
                            else
                            {
                                // replace succeeded, break from loop
                                actualRecordsRead += (await queryResult.CountAsync());
                                offset += limit;
                            }
                        }
                        catch (CouchbaseException exception)
                        {
                            _logger.LogError($"Exception:{exception.Message}");
                            _logger.LogInformation($"Retrying...{5 - retryAttempts}");
                            isError = true;
                        }
                        if (isError)
                        {
                            await Task.Delay(100);
                            continue;
                        }
                        isError = false;
                        _logger.LogInformation($"Loaded {actualRecordsRead} records from {totalRecords} in {stopWatch.Elapsed.TotalMilliseconds} ms");
                        break;
                    }
                }
                stopWatch.Stop();
                pageExecutionTime = stopWatch.ElapsedMilliseconds / iterations;
                _logger.LogInformation($"Load time per page of size {limit} average iteration time is {pageExecutionTime} ms");
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Error - {ex.Message}");
            }
        }

        private async Task GetDataByJoinAsync()
        {
            var batchSize = _configuration.GetValue<int>("CacheKey:BatchSize");
            var offset = _configuration.GetValue<int>("CacheKey:Offset");
            var totalRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            if (totalRecords > batchSize)
            {
                await GetDataByJoinInBatchesAsync(batchSize, offset, totalRecords);
                return;
            }
            await GetXNoOfJSonRecordsByJoinAsync(offset, totalRecords);
        }

        private async Task GetDataByJoinInBatchesAsync(int batchSize,  int offset, int totalRecords)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var noOfBatches = (totalRecords / batchSize) + (totalRecords % batchSize == 0 ? 0 : 1);
            var source = Enumerable.Range(1, noOfBatches).ToArray();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            long totalRecordsRead = 0;
            var joinQuery = "SELECT META(maintenance).id FROM `Cache-Sample`.cache.maintenance maintenance JOIN `Cache-Sample`.cache.common common ON " +
            "maintenance.type = common.criteria1 WHERE maintenance.type = 'maintenance' AND common.type = 'common' OFFSET $offset LIMIT $limit";
            //" maintenance.type = common.criteria1 AND common.type = 'common' WHERE maintenance.type = 'maintenance' and TO_NUMBER(META(maintenance).id) BETWEEN $lowerLimit and $upperLimit";
            //and TO_NUMBER(META(d).id) BETWEEN $lowerLimit and $upperLimit
            stopWatch.Start();
            await source.ParallelForEachAsync(
                  async item =>
                  {
                      try
                      {
                          var lowerLimit = offset + ((item - 1) * batchSize);
                          //var upperLimit = lowerLimit + batchSize;
                          var queryResult = await cluster.QueryAsync<dynamic>(joinQuery, options => options.Metrics(true)
                                                                                      .Parameter("$offset", lowerLimit)
                                                                                      .Parameter("$limit", batchSize));
                          if (queryResult.MetaData.Status != QueryStatus.Success)
                          {
                              ProcessError(queryResult.Errors);
                          }
                          await queryResult.CountAsync();
                          var recordsRead = queryResult.MetaData.Metrics.ResultCount;
                          _logger.LogInformation($"Read {recordsRead} records in {queryResult.MetaData.Metrics.ExecutionTime}");
                          Interlocked.Add(ref totalRecordsRead, (long)recordsRead);
                      }
                      catch (Exception ex)
                      {
                          _logger.LogError(ex.ToString());
                      }

                  },
              Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
          );
            stopWatch.Stop();
            _logger.LogInformation($"Total time elapsed to read {Interlocked.Read(ref totalRecordsRead)} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
        }
        private async Task GetXNoOfJSonRecordsByJoinAsync(int offset, int totalRecords)
        {

            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var joinQuery = "SELECT META(maintenance).id FROM `Cache-Sample`.cache.maintenance maintenance JOIN `Cache-Sample`.cache.common common ON" +
                    " maintenance.type = common.criteria1 WHERE maintenance.type = 'maintenance' AND common.type = 'common' OFFSET $offset LIMIT $limit";
                //and TO_NUMBER(META(d).id) BETWEEN $lowerLimit and $upperLimit
                stopWatch.Start();
                var queryResults = await cluster.QueryAsync<dynamic>(joinQuery, options => options.Metrics(true)
                                                                                        .Parameter("$offset", offset)
                                                                                        .Parameter("$limit", totalRecords));
                if (queryResults.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(queryResults.Errors);
                }
                stopWatch.Stop();
                var filteredRecords = (await queryResults.CountAsync());
                _logger.LogInformation($"Time elapsed to fetch {filteredRecords} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {

            }
        }


        //Ignore below code
        #region Old methods

        private async Task WriteRecords()
        {
            var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * 1000;
            JObject jObject;
            var jsonPath = @"MaintenanceDomain.json";
            using (StreamReader file = File.OpenText(jsonPath))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    jObject = (JObject)JToken.ReadFrom(reader);
                }

            }
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            for (int i = cacheUpperValue - 999; i <= cacheUpperValue; i++)
            {
                try
                {
                    await _cache.SetStringAsync(i.ToString(), jObject.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString()); ;
                }
            }
            stopWatch.Stop();
            Console.WriteLine($"Time elapsed in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");
        }

        private async Task WriteKeyValueRecordsInParallelAsync()
        {
            var maxRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * maxRecords;
            JObject jObject;
            var jsonPath = @"MaintenanceDomain.json";
            using (StreamReader file = File.OpenText(jsonPath))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    jObject = (JObject)JToken.ReadFrom(reader);
                }
            }
            var source = Enumerable.Range(cacheUpperValue - (maxRecords - 1), maxRecords).ToArray();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await source.ParallelForEachAsync(
                async item =>
                {
                    try
                    {
                        await _cache.SetStringAsync(item.ToString(), jObject.ToString());
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex.ToString()); ;
                    }
                },
                Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            );
            stopWatch.Stop();
            _logger.LogInformation($"Time elapsed to insert {source.Length} records in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");

            #region Commented code

            //var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * 10000;
            //JObject jObject;
            //var jsonPath = @"MaintenanceDomain.json";
            //using (StreamReader file = File.OpenText(jsonPath))
            //{
            //    using (JsonTextReader reader = new JsonTextReader(file))
            //    {
            //        jObject = (JObject)JToken.ReadFrom(reader);
            //    }

            //}
            //var source = Enumerable.Range(cacheUpperValue - 9999, cacheUpperValue).ToArray();
            //// Partition the entire source array.
            //var rangePartitioner = Partitioner.Create(0, source.Length);
            //var stopWatch = new Stopwatch();
            //stopWatch.Start();
            //Parallel.ForEach(rangePartitioner, async (range, loopState) =>
            //{
            //    for (int i = range.Item1; i < range.Item2; i++)
            //    {
            //        try
            //        {
            //            await _cache.SetStringAsync(source[i].ToString(), jObject.ToString());
            //        }
            //        catch (Exception ex)
            //        {
            //            Console.WriteLine(ex.ToString()); ;
            //        }
            //    }
            //});
            //stopWatch.Stop();
            //Console.WriteLine($"Time elapsed in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");
            //await Task.Delay(1);

            #endregion
        }

        private async Task ReadRecords()
        {
            var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * 1000;
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            for (int i = cacheUpperValue - 999; i <= cacheUpperValue; i++)
            {
                try
                {
                    var jObject = await _cache.GetStringAsync(i.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString()); ;
                }
            }
            stopWatch.Stop();
            Console.WriteLine($"Time elapsed in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");
        }

        private async Task ReadKeyValueRecordsInParallelAsync()
        {
            var maxRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * maxRecords;
            var source = Enumerable.Range(cacheUpperValue - (maxRecords - 1), maxRecords).ToArray();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            int ctr = 0;
            await source.ParallelForEachAsync(
                async item =>
                {
                    try
                    {
                        var jObject = await _cache.GetStringAsync(item.ToString());
                        if (jObject != null)
                        { Interlocked.Increment(ref ctr); }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString()); ;
                    }
                },
                Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0))
            );
            stopWatch.Stop();
            Console.WriteLine($"Time elapsed to read {ctr} records in miliseconds {stopWatch.Elapsed.TotalMilliseconds}");
        }

        private async Task ReadJsonRecordsUsingAnalyticsAsync()
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.AnalyticsQueryAsync<int>("SELECT VALUE Count(*) FROM `Cache-Sample`.cache.maintenance d where d.`type`= 'maintenance'");

                //var result = await cluster.QueryAsync<dynamic>("SELECT VALUE Count(*) as totalRecords FROM `Cache-Sample`._default._default d where d.`type`= 'maintenance'");                
                if (result.MetaData.Status != Analytics.AnalyticsStatus.Success)
                {
                    ProcessWarnings(result.MetaData.Warnings);
                }
                var totalRecords = await result.FirstAsync<int>();
                var filterQuery = "SELECT VALUE d FROM `Cache-Sample`.cache.maintenance d where d.`type`= 'maintenance'";
                stopWatch.Start();
                var filteredResults = await cluster.AnalyticsQueryAsync<dynamic>(filterQuery);
                if (filteredResults.MetaData.Status != Analytics.AnalyticsStatus.Success)
                {
                    ProcessWarnings(result.MetaData.Warnings);
                }
                var filteredRecords = (await filteredResults.CountAsync());
                stopWatch.Stop();
                _logger.LogInformation($"Time elapsed to filter {filteredRecords} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {

            }
        }

        private async Task ReadSortedJsonRecordsAsync()
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var totalRecords = (await result.FirstAsync()).totalRecords;
                var filterQuery = "SELECT d.* FROM `Cache-Sample`.cache.maintenance AS d ORDER BY d.Id";
                stopWatch.Start();
                var filteredResults = await cluster.QueryAsync<dynamic>(filterQuery);
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var filteredRecords = (await filteredResults.CountAsync());
                stopWatch.Stop();
                _logger.LogInformation($"Time elapsed to filter {filteredRecords} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {

            }
        }

        private async Task BulkReadJsonRecordsWithAnalyticsByPaginationAsync()
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.AnalyticsQueryAsync<int>("SELECT VALUE Count(*) FROM `Cache-Sample`.cache.maintenance d where d.`type`= 'maintenance'");

                //var result = await cluster.QueryAsync<dynamic>("SELECT VALUE Count(*) as totalRecords FROM `Cache-Sample`._default._default d where d.`type`= 'maintenance'");                
                if (result.MetaData.Status != Analytics.AnalyticsStatus.Success)
                {
                    ProcessWarnings(result.MetaData.Warnings);
                }
                var totalRecords = await result.FirstAsync<int>();
                decimal offset = 0;
                var limit = _configuration.GetValue<int>("CacheKey:PageSize");
                var actualRecordsRead = 0;
                var isError = false;
                stopWatch.Start();
                while (offset < totalRecords)
                {
                    var attempts = 5; // eg 5
                                      //var filterQuery = "Select d.* from `Cache-Sample`.cache.maintenance as d where d.type=$type ORDER BY TO_NUMBER(META(d).id) OFFSET $offset LIMIT $limit";
                    var filterQuery = "Select d.* from `Cache-Sample`.cache.maintenance as d where d.type=$type ORDER BY TO_NUMBER(META(d).id) OFFSET $offset LIMIT $limit";
                    while (attempts-- > 0)
                    {
                        try
                        {
                            var filteredResults = await cluster.AnalyticsQueryAsync<dynamic>(filterQuery, options =>
                        options.Parameter("$type", "maintenance")
                        .Parameter("$offset", offset));
                            //.Parameter("$limit", limit));
                            if (filteredResults.MetaData.Status != Analytics.AnalyticsStatus.Success)
                            {
                                ProcessWarnings(result.MetaData.Warnings);
                            }
                            else
                            {
                                // replace succeeded, break from loop
                                actualRecordsRead += (await filteredResults.CountAsync());
                                offset += limit;
                            }
                        }
                        catch (CouchbaseException exception)
                        {
                            _logger.LogError($"Exception:{exception.Message}");
                            _logger.LogInformation($"Retrying...{5 - attempts}");
                            isError = true;
                        }
                        if (isError)
                        {
                            await Task.Delay(100);
                            continue;
                        }
                        isError = false;
                        await Task.Delay(100);
                        _logger.LogInformation($"Loaded {actualRecordsRead} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
                        break;
                    }

                }
                stopWatch.Stop();
                _logger.LogInformation($"Loaded {actualRecordsRead} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {

            }
        }

        private async Task FilterAsync()
        {

            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var totalRecords = (await result.FirstAsync()).totalRecords;
                var filterQuery = "SELECT COUNT(*) as matchCount FROM `Cache-Sample`.cache.maintenance AS d UNNEST d.DataEnumerable1 AS arr WHERE arr.CreatedBy=$createdBy";
                var criteria = "corp\\subasish";
                stopWatch.Start();
                var filteredResults = await cluster.QueryAsync<dynamic>(filterQuery,
        options => options.Parameter("$createdBy", criteria));
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var filteredRecords = (await filteredResults.FirstAsync()).matchCount;
                stopWatch.Stop();
                //filterQuery = "SELECT COUNT(*) as matchCount FROM `Cache-Sample`.cache.maintenance AS d UNNEST d.DataEnumerable1 AS arr WHERE arr.CreatedBy = 'corp\\\\subasish'";
                //var filteredQueryResults = await cluster.QueryAsync<dynamic>(filterQuery);
                //if (result.MetaData.Status != QueryStatus.Success)
                //{
                //    ProcessError(result.Errors);
                //}
                //filteredRecords = (await filteredQueryResults.FirstAsync()).matchCount;
                _logger.LogInformation($"Time elapsed to filter {filteredRecords} records from {totalRecords} is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {

            }
        }

        private async Task DeleteAllRecordsAsync()
        {

            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var totalRecords = (await result.FirstAsync()).totalRecords;
                var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type";
                stopWatch.Start();
                result = await cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance"));
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                stopWatch.Stop();
                result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var totalRecordsAfterDeleteAll = (await result.FirstAsync()).totalRecords;
                _logger.LogInformation($"Time elapsed to delete {totalRecords - totalRecordsAfterDeleteAll} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {
            }
        }

        private async Task DeleteAllRecordsInBatchesAsync()
        {

            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var totalRecords = (await result.FirstAsync()).totalRecords;
                var source = Enumerable.Range(1, (int)totalRecords).ToArray();
                stopWatch.Start();
                var rangePartitioner = Partitioner.Create(0, source.Length);
                Parallel.ForEach(rangePartitioner, (range, loopState) =>
                {
                    var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type and d.docId BETWEEN $lower AND $upper";
                    var result = cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                                                                                       .Parameter("$lower", range.Item1)
                                                                                       .Parameter("$upper", range.Item2)).Result;
                    //var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type LIMIT $range";
                    //var result = cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                    //                                                                   .Parameter("$range", range.Item2-range.Item1+1)).Result;
                    if (result.MetaData.Status != QueryStatus.Success)
                    {
                        ProcessError(result.Errors);
                    }
                });
                stopWatch.Stop();
                //result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                //if (result.MetaData.Status != QueryStatus.Success)
                //{
                //    ProcessError(result.Errors);
                //}
                //var totalRecordsAfterDeleteAll = (await result.FirstAsync()).totalRecords;
                _logger.LogInformation($"Time elapsed to delete {source.Length} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {
            }
        }

        private async Task DeleteClientRecordsInBatchesOf5kAsync()
        {
            var maxRecords = _configuration.GetValue<int>("CacheKey:RecordCount");
            var cacheUpperValue = _configuration.GetValue<int>("CacheKey:UpperValue") * maxRecords;
            var cacheLowerValue = cacheUpperValue - (maxRecords - 1);
            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var source = Enumerable.Range(cacheLowerValue, maxRecords).ToArray();
                var rangePartitioner = Partitioner.Create(0, source.Length);
                stopWatch.Start();
                Parallel.ForEach(rangePartitioner, (range, loopState) =>
                {
                    var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type and d.docId BETWEEN $lower AND $upper";
                    var result = cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                                                                                       .Parameter("$lower", range.Item1)
                                                                                       .Parameter("$upper", range.Item2)).Result;
                    //var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type LIMIT $range";
                    //var result = cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                    //                                                                   .Parameter("$range", range.Item2-range.Item1+1)).Result;
                    if (result.MetaData.Status != QueryStatus.Success)
                    {
                        ProcessError(result.Errors);
                    }
                });
                stopWatch.Stop();
                _logger.LogInformation($"Time elapsed to delete {source.Length} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {
            }
        }

        private async Task DeleteAllRecordsInBatchesOf5kAsync()
        {

            var cluster = await _clusterProvider.GetClusterAsync();
            var stopWatch = new Stopwatch();
            try
            {
                var result = await cluster.QueryAsync<dynamic>("SELECT Count(*) as totalRecords FROM `Cache-Sample`.cache.maintenance");
                if (result.MetaData.Status != QueryStatus.Success)
                {
                    ProcessError(result.Errors);
                }
                var totalRecords = (await result.FirstAsync()).totalRecords;
                var source = Enumerable.Range(1, (int)totalRecords).ToArray();

                // collection of things that will complete in the future
                var tasks = new List<Task>();
                var loops = (source.Length / 5000) + (source.Length % 5000 == 0 ? 0 : 1);

                // create tasks to be executed concurrently
                // NOTE: these tasks have not yet been scheduled
                for (var i = 0; i < loops; i++)
                {
                    var startRange = (5000 * i);
                    var lowerRange = startRange + 1;
                    var upperRange = startRange + 5000;
                    var filterQuery = "DELETE FROM `Cache-Sample`.cache.maintenance AS d WHERE d.type = $type and d.docId BETWEEN $lower AND $upper";
                    var task = cluster.QueryAsync<dynamic>(filterQuery, options => options.Parameter("$type", "maintenance")
                                                                                       .Parameter("$lower", lowerRange)
                                                                                       .Parameter("$upper", upperRange));
                    tasks.Add(task);
                }

                stopWatch.Start();
                // Waits until all of the tasks have completed
                await Task.WhenAll(tasks);
                stopWatch.Stop();
                _logger.LogInformation($"Time elapsed to delete {source.Length} records is {stopWatch.Elapsed.TotalMilliseconds} ms");
            }
            catch (Exception ex)
            {
            }
        }

        private void ProcessError(List<Error> errors)
        {
            var errorLog = "";
            foreach (var error in errors)
                errorLog += error.Message + Environment.NewLine;
            _logger.LogError(errorLog);
        }
        private void ProcessWarnings(List<Analytics.AnalyticsWarning> warnings)
        {
            var warningLog = "";
            foreach (var warning in warnings)
                warningLog += warning.Message + Environment.NewLine;
            _logger.LogError(warningLog);
        }

        #region GetAndSet string object

        ////[Route("GetCacheDataAsync/{cacheKey}")]
        //public async Task<string> GetCacheDataAsync(string cacheKey)
        //{
        //    var data = await _cache.GetStringAsync(cacheKey);
        //    if (data == null)
        //    {
        //        data = DateTime.Now + "/" + Guid.NewGuid();
        //        await _cache.SetStringAsync(cacheKey, data);
        //    }
        //    return data;
        //}

        #endregion

        #region GetAndSetComplexObject

        //public async Task<WeatherForecast> GetCachedWeatherForecastAsync()
        //{
        //    var cacheKey = DateTime.Now.Date.ToShortDateString();
        //    var weatherForecast = new WeatherForecast() { Date = DateTime.Now, Summary = "Sample weather", TemperatureC = 35 };
        //    byte[] utf8bytesJson = JsonSerializer.SerializeToUtf8Bytes(weatherForecast);
        //    try
        //    {
        //        weatherForecast = await _cache.GetAsync<WeatherForecast>(cacheKey);
        //    }
        //    catch (CouchbaseException exception)
        //    {
        //        var options = new DistributedCacheEntryOptions() { AbsoluteExpirationRelativeToNow = TimeSpan.FromDays(10) };
        //        await _cache.SetAsync<WeatherForecast>(cacheKey, weatherForecast, options);
        //        //string jsonString = JsonSerializer.Serialize(new WeatherForecast() { Date = DateTime.Now.Date, Summary = $"Forecast for {DateTime.Now.Date}", TemperatureC = new Random().Next(0, 50) });
        //        //await _cache.SetStringAsync(cacheKey, jsonString);
        //        weatherForecast = await _cache.GetAsync<WeatherForecast>(cacheKey);
        //    }
        //    return weatherForecast;
        //}
        #endregion

        #endregion
    }
}



