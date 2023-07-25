using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;
using System;
using Microsoft.Azure.Cosmos;
using Container = Microsoft.Azure.Cosmos.Container;
using System.Linq;
using Microsoft.Azure.Cosmos.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    public static class CachePatternListTestFunctions
    {
        public const string localhostSetting = "redisLocalhost";
        public const string Endpoint = "Endpoint";
        public const int pollingInterval = 100;
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        //CosmosDB database name and container name declared here
        public const string databaseName = "databaseName";
        public const string containerName = "containerName";

        //Uses the key of the user's choice and should be changed accordingly
        public const string key = "userListName";

        public static async Task ToCacheAsync(FeedResponse<ListData> response, ListData item, string listEntry)
        {
            var fullEntry = response.Take(response.Count);

            if (fullEntry == null) return;

            foreach (ListData inputValues in fullEntry)
            {
                RedisValue[] redisValues = Array.ConvertAll(inputValues.value.ToArray(), item => (RedisValue)item);
                await cache.ListRightPushAsync(listEntry, redisValues);
            }
        }

        [FunctionName("CosmosToRedis")]
        public static void Run([CosmosDBTrigger(
        databaseName: "%databaseName%",
        containerName: "%containerName%",
        Connection = "Endpoint",
        LeaseContainerName = "leases")]IReadOnlyList<ListData> readOnlyList, ILogger log)
        {
            if (readOnlyList == null || readOnlyList.Count <= 0) return;

            foreach (ListData inputValues in readOnlyList)
            {
                if (inputValues.id == key)
                {
                    RedisValue[] redisValues = Array.ConvertAll(inputValues.value.ToArray(), item => (RedisValue)item);
                    cache.ListRightPush(key, redisValues);
                }
            }
        }

        [FunctionName(nameof(ListTriggerAsync))]
        public static async Task ListTriggerAsync(
             [RedisListTrigger(localhostSetting, key)] string listEntry, [CosmosDB(
            Connection = "Endpoint" )]CosmosClient client,
             ILogger logger)
        {
            Container db = client.GetDatabase(Environment.GetEnvironmentVariable(databaseName)).GetContainer(Environment.GetEnvironmentVariable(containerName));

            IOrderedQueryable<ListData> query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == key)
                .ToFeedIterator();

            FeedResponse<ListData> response = await results.ReadNextAsync();
            ListData item = response.FirstOrDefault(defaultValue: null);

            List<string> resultsHolder = item?.value ?? new List<string>();

            resultsHolder.Add(listEntry);
            ListData newEntry = new ListData(id: key, value: resultsHolder);
            await db.UpsertItemAsync<ListData>(newEntry);
        }

        [FunctionName(nameof(ListTriggerReadThroughFunc))]
        public static async Task ListTriggerReadThroughFunc(
            [RedisPubSubTrigger(localhostSetting, "__keyevent@0__:keymiss")] string listEntry, [CosmosDB(
            Connection = "Endpoint" )]CosmosClient client,
            ILogger logger)
        {
            Container db = client.GetDatabase(Environment.GetEnvironmentVariable(databaseName)).GetContainer(Environment.GetEnvironmentVariable(containerName));

            IOrderedQueryable<ListData> query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == listEntry)
                .ToFeedIterator();

            FeedResponse<ListData> response = await results.ReadNextAsync();
            ListData item = response.FirstOrDefault(defaultValue: null);

            if (item == null) return;
            else
            {
                await ToCacheAsync(response, item, listEntry);
            }
        }
    }
}

