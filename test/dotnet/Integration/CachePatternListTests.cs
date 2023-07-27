using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Logging;
using Azure;
using System.ComponentModel;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    [Collection("RedisTriggerTests")]
    public class CachePatternListTests
    {
        //Replace with desired key name
        string key = "userListName";

        //Replace DatabaseName and ContainerName with user's info
        public const string CosmosDbDatabaseID = "CosmosDbDatabaseID";
        public const string CosmosDbContainerID = "CosmosDbContainerID";

        //Replace with number of values associated to the key in cosmos
        int iterations = 2;

        [Fact]
        public async void ListsTrigger_WriteBack()
        {
            string functionName = nameof(CachePatternListTestFunctions.ListTriggerAsync);
            RedisValue[] valuesArray = new RedisValue[] { "a", "b" };

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.connectionString)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = CachePatternListIntegrationTestHelpers.StartFunction(functionName, 7071))
                {
                    functionsProcess.OutputDataReceived += CachePatternListIntegrationTestHelpers.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync(key, valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(3));

                    await multiplexer.CloseAsync();
                    functionsProcess.Kill();
                };
                var incorrect = counts.Where(pair => pair.Value != 0);
                Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
            }
        }

        [Fact]
        public async void ListsTrigger_WriteBackHeavyLoading()
        {
            string functionName = nameof(CachePatternListTestFunctions.ListTriggerAsync);
            RedisValue[] valuesArray = new RedisValue[2000];
            for (int i = 0; i < valuesArray.Length; i++)
            {
                valuesArray[i] = i;
            }

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.connectionString)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = CachePatternListIntegrationTestHelpers.StartFunction(functionName, 7071))
                {
                    functionsProcess.OutputDataReceived += CachePatternListIntegrationTestHelpers.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync(key, valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(2566));

                    await multiplexer.CloseAsync();
                    functionsProcess.Kill();
                };
                var incorrect = counts.Where(pair => pair.Value != 0);
                Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
            }
        }

        [Fact]
        public async void ListsTrigger_InCosmos()
        {
            CosmosClientBuilder clientBuilder = new CosmosClientBuilder(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.cosmosDBConnectionString));
            CosmosClient cosmosClient = clientBuilder.Build();

            Container db = cosmosClient.GetDatabase(CosmosDbDatabaseID).GetContainer(CosmosDbContainerID);
            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == key)
                .ToFeedIterator();

            var response = await results.ReadNextAsync();
            var item = response.FirstOrDefault(defaultValue: null);

            await Task.Delay(TimeSpan.FromSeconds(5));

            Assert.Equal(item.id, key);
        }

        [Fact]
        public async void ListsTrigger_CosmosToRedis()
        {
            ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.connectionString));
            bool exists = true;

            CosmosClientBuilder clientBuilder = new CosmosClientBuilder(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.cosmosDBConnectionString));
            CosmosClient cosmosClient = clientBuilder.Build();

            Container db = cosmosClient.GetDatabase(CosmosDbDatabaseID).GetContainer(CosmosDbContainerID);

            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == key)
                .ToFeedIterator();

            FeedResponse<ListData> response = await results.ReadNextAsync();
            ListData item = response.FirstOrDefault(defaultValue: null);

            var fullEntry = response.Take(response.Count);

            if (fullEntry == null) return;

            foreach (ListData inputValues in fullEntry)
            {
                RedisValue[] redisValues = Array.ConvertAll(inputValues.value.ToArray(), item => (RedisValue)item);
                await multiplexer.GetDatabase().ListRightPushAsync(key, redisValues);

            }

            await Task.Delay(TimeSpan.FromSeconds(10));

            exists = await multiplexer.GetDatabase().KeyExistsAsync(key);

            Assert.True(exists);
        }




        [Fact]
        public async void ListsTrigger_ReadThrough()
        {
            ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.connectionString));
            string functionName = nameof(CachePatternListTestFunctions.ListTriggerReadThroughFunc);
            bool exists = true;

            multiplexer.GetDatabase().KeyDelete(key);

            using (Process functionsProcess = CachePatternListIntegrationTestHelpers.StartFunction(functionName, 7071))
            {
                for (int i = 0; i < iterations; i++)
                {
                    multiplexer.GetDatabase().ListRange(key, 0, -1);
                    await Task.Delay(TimeSpan.FromSeconds(4));
                    if (i != iterations - 1)
                    {
                        multiplexer.GetDatabase().KeyDelete(key);
                    }
                }

                exists = multiplexer.GetDatabase().KeyExists(key);

                multiplexer.Close();
                functionsProcess.Kill();

                Assert.True(exists);
            }

        }

    }
}


