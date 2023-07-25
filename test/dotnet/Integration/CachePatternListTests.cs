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

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    [Collection("RedisTriggerTests")]
    public class CachePatternListTests
    {
        public const string databaseName = "databaseName";
        public const string containerName = "containerName";
        string key = "userListName";
        [Fact]
        public async void ListsTrigger_WriteBack()
        {
            string functionName = nameof(CachePatternListTestFunctions.ListTriggerAsync);
            RedisValue[] valuesArray = new RedisValue[] { "a", "b" };

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.localhostSetting)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = CachePatternListIntegrationTestHelpers.StartFunction(functionName, 7071))
                {
                    functionsProcess.OutputDataReceived += CachePatternListIntegrationTestHelpers.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync(key, valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(5));

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
            RedisValue[] valuesArray = new RedisValue[200];
            for (int i = 0; i < valuesArray.Length; i++)
            {
                valuesArray[i] = "test" + i;
            }

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.localhostSetting)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = CachePatternListIntegrationTestHelpers.StartFunction(functionName, 7071))
                {
                    functionsProcess.OutputDataReceived += CachePatternListIntegrationTestHelpers.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync(key, valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(256));

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

            CosmosClientBuilder clientBuilder = new CosmosClientBuilder(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.Endpoint));
            CosmosClient cosmosClient = clientBuilder.Build();

            //Replace DatabaseName and ContainerName with user's info
            Container db = cosmosClient.GetDatabase(databaseName).GetContainer(containerName);

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
            CosmosClientBuilder clientBuilder = new CosmosClientBuilder(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.Endpoint));
            CosmosClient cosmosClient = clientBuilder.Build();
            ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.localhostSetting));
            bool exists = true;

            //Replace DatabaseName and ContainerName with user's info
            Container db = cosmosClient.GetDatabase(databaseName).GetContainer(containerName);

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
            string functionName = nameof(CachePatternListTestFunctions.ListTriggerReadThroughFunc);
          
            bool exists;

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(CachePatternListIntegrationTestHelpers.localsettings, CachePatternListTestFunctions.localhostSetting)))
            using (Process functionsProcess = CachePatternListIntegrationTestHelpers.StartFunction(functionName, 7071))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                long listLength = await multiplexer.GetDatabase().ListLengthAsync(key);
                await multiplexer.GetDatabase().ListRangeAsync(key, 0, listLength - 1);

                await Task.Delay(TimeSpan.FromSeconds(5));

                exists = await multiplexer.GetDatabase().KeyExistsAsync(key);
                await multiplexer.CloseAsync();
                functionsProcess.Kill();

                Assert.True(exists);
            }
        }

    }
}

