using Couchbase.Extensions.Caching;
using Couchbase.Extensions.DependencyInjection;
using Couchbase.KeyValue;
using Couchbase.Query;
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
using System.Threading;
using System.Threading.Tasks;

namespace Couchbase.BulkReadAndWrite
{
    public class PlanScenarioBackgroundWorker : BackgroundService
    {
        private readonly ILogger<PlanScenarioBackgroundWorker> _logger;
        private IConfiguration _configuration;
        private readonly IClusterProvider _clusterProvider;
        private readonly int MaxRetries = 5;

        public PlanScenarioBackgroundWorker(ILogger<PlanScenarioBackgroundWorker> logger, IConfiguration configuration, IClusterProvider clusterProvider)
        {
            _logger = logger;
            _configuration = configuration;
            _clusterProvider = clusterProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                var times=_configuration.GetValue<int>("Iteration");
                //await Enumerable.Range(1, times).ParallelForEachAsync(async x => await CreateScenarioWithSchedulesMappedAsync());
                foreach (var iteration in Enumerable.Range(1, times))
                {
                    await CreateScenarioWithSchedulesMappedAsync();
                }

                //var scenarioId = await CreateScenarioWithSchedulesMappedAsync();
                //await CopyScenarioWithSchedulesMapped(scenarioId);
                //await CopyScenarioWithSchedulesMapped();
                //await ReadFullScenario();
            }
            catch (Exception ex)
            {

            }
        }

        #region Read scenario

        private async Task ReadFullScenario(Guid? readScenarioId = null)
        {
            Guid scenarioId = readScenarioId ?? _configuration.GetValue<Guid>("ScenarioId");
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await cluster.BucketAsync("RailMAX-Master");
            var masterScope = await bucket.ScopeAsync("Master");
            var scenarioCollection = await masterScope.CollectionAsync("Scenarios");
            var scheduleCollection = await masterScope.CollectionAsync("Schedules");
            
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            var scenarioResult = await scenarioCollection.GetAsync(scenarioId.ToString());
            var scenario = scenarioResult.ContentAs<dynamic>();

            var schedules = new List<dynamic>();
            var getScheduleQuery = "Select RAW s.scheduleId as scheduleIds from `RailMAX-Master`.Master.ScenarioScheduleMapping AS d UNNEST schedules AS s WHERE d.type = $type " +
              "AND d.scenarioId= $scenarioId";
            var getSchedulesResult = await cluster.QueryAsync<dynamic>(
              getScheduleQuery, options => options
                .Parameter("type", "ScenarioScheduleMapping")
                .Parameter("scenarioId", scenarioId));
            var newScheduleIds = new ConcurrentBag<Guid>();
            await getSchedulesResult.Rows.ParallelForEachAsync(
           async scheduleId =>
           {
               var scheduleResult = await scheduleCollection.GetAsync((string)scheduleId);
               var schedule = scheduleResult.ContentAs<dynamic>();
               schedules.Add(schedule);
           });
            stopWatch.Stop();
            _logger.LogInformation($"Time required to read scenario id:{scenarioId} is {stopWatch.Elapsed.TotalMilliseconds} ms");
        }

        #endregion

        #region CreateScenariosWithSchedules

        private async Task<Guid> CreateScenarioWithSchedulesMappedAsync()
        {
            var scenarioId = Guid.NewGuid();
            var getSchedulesData = await CreateJsonSchedulesAsync();
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            var createScenarioTask = CreateScenarioAsync(scenarioId);
            //await Task.WhenAll(createScenarioTask, createSchedulesAndMappingTask);
            //var results = await createSchedulesAndMappingTask;
            Task createSchedulesTask = CreateSchedulesForScenariosAsync(getSchedulesData);
            Task mapScenarioWithSchedulesTask = MapScenarioWithSchedulesAsync(scenarioId, getSchedulesData);
            await Task.WhenAll(createScenarioTask, createSchedulesTask, mapScenarioWithSchedulesTask);
            stopWatch.Stop();
            _logger.LogInformation($"Time elapsed in creating scenario \"{scenarioId}\": {stopWatch.Elapsed.TotalMilliseconds} ms");
            return scenarioId;
        }

        private async Task CreateScenarioAsync(Guid scenarioId)
        {
            JObject scenarioJObject;
            var jsonPath = @"SampleData1.json";
            using (StreamReader file = File.OpenText(jsonPath))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    scenarioJObject = (JObject)JToken.ReadFrom(reader);
                }
            }
            scenarioJObject.Add("id", scenarioId);
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await cluster.BucketAsync("RailMAX-Master");
            var masterScope = await bucket.ScopeAsync("Master");
            var scenariosCollection = await masterScope.CollectionAsync("Scenarios");
            var attempts = MaxRetries;
            while (attempts-- > 0)
            {
                try
                {
                    await scenariosCollection.InsertAsync<JObject>(scenarioId.ToString(), scenarioJObject);
                    break;
                }
                catch (CouchbaseException exception)
                {
                    _logger.LogError(exception.Message);
                    switch (exception)
                    {
                        // unrecoverable error (network failure, etc)
                        case NetworkErrorException _:
                            throw;
                            //case other unrecoverable exceptions
                    }
                }
                // wait 100 milliseconds before trying again
                await Task.Delay(100);
            }
            _logger.LogInformation($"Scenario id:{scenarioId} created");
        }

        private async Task<int> CreateSchedulesForScenariosAsync(List<(Guid id, JObject content)> schedules)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await cluster.BucketAsync("RailMAX-Master");
            var masterSccope = await bucket.ScopeAsync("Master");
            var schedulesCollection = await masterSccope.CollectionAsync("Schedules");
            int recordCount = 0;
            await schedules.ParallelForEachAsync(
                async schedule =>
                {
                    var attempts = MaxRetries; // eg 5
                    while (attempts-- > 0)
                    {
                        try
                        {
                            var result = await schedulesCollection.InsertAsync(schedule.id.ToString(), schedule.content);
                            Interlocked.Increment(ref recordCount);
                            break;
                        }
                        catch (CouchbaseException exception)
                        {
                            _logger.LogError(exception.Message);
                            switch (exception)
                            {
                                // unrecoverable error (network failure, etc)
                                case NetworkErrorException _:
                                    throw;
                                    //case other unrecoverable exceptions
                            }
                        }
                        // wait 100 milliseconds before trying again
                        await Task.Delay(100);
                    }

                }, Environment.ProcessorCount);
            _logger.LogInformation($"{recordCount} schedules created");
            return recordCount;
        }

        private async Task<List<(Guid id, JObject content)>> CreateJsonSchedulesAsync()
        {
            JObject scheduleJObject;
            var scheduleCount = _configuration.GetValue<int>("ScheduleCount");
            var jsonPath = @"SampleData2.json";
            using (StreamReader file = File.OpenText(jsonPath))
            {
                using (JsonTextReader reader = new JsonTextReader(file))
                {
                    var jToken = await JToken.ReadFromAsync(reader);
                    scheduleJObject = jToken.ToObject<JObject>();
                }
            }
            return Enumerable.Range(1, scheduleCount).Select(item =>
            {
                var content = JObject.FromObject(scheduleJObject);
                var id = Guid.NewGuid();
                content.Add("id", id);
                return (id, content);
            }).ToList();
        }

        private async Task MapScenarioWithSchedulesAsync(Guid scenarioId, List<(Guid id, JObject content)> schedulesCollection)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await cluster.BucketAsync("RailMAX-Master");
            var masterScope = await bucket.ScopeAsync("Master");
            var scenarioScheduleMappingCollection = await masterScope.CollectionAsync("ScenarioScheduleMapping");
            var scenarioScheduleContent = new
            {
                type = "ScenarioScheduleMapping",
                scenarioId = scenarioId,
                schedules = schedulesCollection.Select(schedule => new
                {
                    scheduleId = schedule.id,
                    isActive = true
                }).ToArray()
            };
            var attempts = MaxRetries; // eg 5
            while (attempts-- > 0)
            {
                try
                {
                    await scenarioScheduleMappingCollection.InsertAsync(Guid.NewGuid().ToString(), scenarioScheduleContent);
                    break;
                }
                catch (CouchbaseException exception)
                {
                    _logger.LogError(exception.Message);
                    switch (exception)
                    {
                        // unrecoverable error (network failure, etc)
                        case NetworkErrorException _:
                            throw;
                            //case other unrecoverable exceptions
                    }
                }
                // wait 100 milliseconds before trying again
                await Task.Delay(100);
            }
            _logger.LogInformation($"{scenarioScheduleContent.schedules.Count()} schedules created for scenario id:{scenarioId}");
        }

        #endregion

        #region Copy scenario

        private async Task CopyScenarioWithSchedulesMapped(Guid? scenarioId = null)
        {
            Guid oldScenarioId = scenarioId ?? _configuration.GetValue<Guid>("ScenarioId");
            var newScenarioId = Guid.NewGuid();

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            Task copyScenarioTask = CopyScenarioAsync(oldScenarioId, newScenarioId);
            Task copySchedulesTask = CopySchedulesAsync(oldScenarioId, newScenarioId);
            await Task.WhenAll(copyScenarioTask, copySchedulesTask);
            stopWatch.Stop();
            _logger.LogInformation($"Scenario {oldScenarioId} schedules copied to scenario id:{newScenarioId} in {stopWatch.Elapsed.TotalMilliseconds} ms");
        }

        private async Task CopyScenarioAsync(Guid oldScenarioId, Guid newScenarioId)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await cluster.BucketAsync("RailMAX-Master");
            var masterScope = await bucket.ScopeAsync("Master");
            var scenarioCollection = await masterScope.CollectionAsync("Scenarios");
            var attempts = MaxRetries; // eg 5
            while (attempts-- > 0)
            {
                try
                {
                    var copyScenarioInsertQuery = "INSERT INTO `RailMAX-Master`.Master.Scenarios(KEY v.id, VALUE v) " +
                "SELECT OBJECT_PUT(d, \"id\", newkey) AS v  FROM  `RailMAX-Master`.Master.Scenarios AS d USE KEYS $oldScenarioId " +
                "LET newkey = $newScenarioId";
                    var copyScenarioQueryResults = await cluster.QueryAsync<dynamic>(
                      copyScenarioInsertQuery, options => options
                        .Parameter("oldScenarioId", oldScenarioId.ToString())
                        .Parameter("newScenarioId", newScenarioId.ToString())
                    );
                    break;
                }
                catch (CouchbaseException exception)
                {
                    _logger.LogError(exception.Message);
                    switch (exception)
                    {
                        // unrecoverable error (network failure, etc)
                        case NetworkErrorException _:
                            throw;
                            //case other unrecoverable exceptions
                    }
                }
                // wait 100 milliseconds before trying again
                await Task.Delay(100);
            }
        }

        private async Task CopySchedulesAsync(Guid oldScenarioId, Guid newScenarioId)
        {
            var cluster = await _clusterProvider.GetClusterAsync();
            var bucket = await cluster.BucketAsync("RailMAX-Master");
            var masterScope = await bucket.ScopeAsync("Master");
            var scenarioCollection = masterScope.Collection("Scenarios");
            var scheduleCollection = masterScope.Collection("Schedules");
            var scenarioScheduleMappingCollection = masterScope.Collection("ScenarioScheduleMapping");
            //Insert copied schedules
            var getScheduleQuery = "Select RAW s.scheduleId as scheduleIds from `RailMAX-Master`.Master.ScenarioScheduleMapping AS d UNNEST schedules AS s WHERE d.type = $type " +
                "AND d.scenarioId= $oldScenarioId";
            var getSchedulesResult = await cluster.QueryAsync<dynamic>(
              getScheduleQuery, options => options
                .Parameter("type", "ScenarioScheduleMapping")
                .Parameter("oldScenarioId", oldScenarioId));
            var newScheduleIds = new ConcurrentBag<Guid>();
            //await foreach (var scheduleId in getSchedulesResult.Rows)
            await getSchedulesResult.Rows.ParallelForEachAsync(
           async scheduleId =>
               {
                   var attempts = MaxRetries; // eg 5
                   while (attempts-- > 0)
                   {
                       try
                       {
                           var newScheduleId = Guid.NewGuid();
                           var copySchedulesQuery = "INSERT INTO `RailMAX-Master`.Master.Schedules(KEY v.id, VALUE v) " +
                         "SELECT OBJECT_PUT(d, \"id\", newkey) AS v  FROM  `RailMAX-Master`.Master.Schedules AS d USE KEYS $oldScheduleId " +
                         "LET newkey = $newScheduleId";
                           var copySchedulesQueryResults = await cluster.QueryAsync<dynamic>(
                         copySchedulesQuery, options => options
                           .Parameter("$oldScheduleId", scheduleId)
                           .Parameter("$newScheduleId", newScheduleId));
                           newScheduleIds.Add(newScheduleId);
                           break;
                       }
                       catch (CouchbaseException exception)
                       {
                           _logger.LogError(exception.Message);
                           switch (exception)
                           {
                               // unrecoverable error (network failure, etc)
                               case NetworkErrorException _:
                                   throw;
                                   //case other unrecoverable exceptions
                           }
                       }
                       // wait 100 milliseconds before trying again
                       await Task.Delay(100);
                   }
               });
            var attempts = MaxRetries; // eg 5
            while (attempts-- > 0)
            {
                try
                {
                    var scenarioScheduleContent = new
                    {
                        type = "ScenarioScheduleMapping",
                        scenarioId = newScenarioId,
                        schedules = newScheduleIds.Select(scheduleId => new
                        {
                            scheduleId = scheduleId,
                            isActive = true
                        }).ToArray()
                    };
                    await scenarioScheduleMappingCollection.InsertAsync(Guid.NewGuid().ToString(), scenarioScheduleContent);
                    break;
                }
                catch (CouchbaseException exception)
                {
                    _logger.LogError(exception.Message);
                    switch (exception)
                    {
                        // unrecoverable error (network failure, etc)
                        case NetworkErrorException _:
                            throw;
                            //case other unrecoverable exceptions
                    }
                }
                // wait 100 milliseconds before trying again
                await Task.Delay(100);
            }
        }
        #endregion
    }
}


