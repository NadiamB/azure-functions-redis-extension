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
    public static class RedisListTriggerTestFunctionsCopy
    {
        public const string localhostSetting = "redisLocalhost";
        public const int pollingInterval = 100;
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        //CosmosDB database name and container name declared here
        public const string databaseName = "back";
        public const string containerName = "async";

        //Uses the key of the user's choice and should be changed accordingly
        public const string key = "listTest";

        public static async Task toCacheAsync(FeedResponse<ListData> response, ListData item, string listEntry)
        {
            //Retrieve the values in cosmos associated with the list name, so you can access each item
            var fullEntry = response.Take(response.Count);

            if (fullEntry == null) return;

            //Accessing each value from the entry 
            foreach (ListData inputValue in fullEntry)
            {
                RedisValue[] redisValues = Array.ConvertAll(inputValue.value.ToArray(), item => (RedisValue)item);
                foreach (var entryValue in redisValues)
                {
                    //Push key with values into the cache, this variable is specified by the user
                    await cache.ListRightPushAsync(listEntry, entryValue);
                }
            }
        }

        [FunctionName(nameof(ListTrigger_RedisValue))]
        public static void ListTrigger_RedisValue(
            [RedisListTrigger(localhostSetting, nameof(ListTrigger_RedisValue), pollingIntervalInMs: pollingInterval)] RedisValue entry,
            ILogger logger)
        {
            logger.LogInformation(IntegrationTestHelpersCopy.GetLogValue(entry));
        }

        [FunctionName(nameof(ListTrigger_String))]
        public static void ListTrigger_String(
            [RedisListTrigger(localhostSetting, nameof(ListTrigger_String), pollingIntervalInMs: pollingInterval)] string entry,
            ILogger logger)
        {
            logger.LogInformation(IntegrationTestHelpersCopy.GetLogValue(entry));
        }

        [FunctionName(nameof(ListTrigger_ByteArray))]
        public static void ListTrigger_ByteArray(
            [RedisListTrigger(localhostSetting, nameof(ListTrigger_ByteArray), pollingIntervalInMs: pollingInterval)] byte[] entry,
            ILogger logger)
        {
            logger.LogInformation(IntegrationTestHelpersCopy.GetLogValue(entry));
        }

        [FunctionName(nameof(ListTrigger_CustomType))]
        public static void ListTrigger_CustomType(
            [RedisListTrigger(localhostSetting, nameof(ListTrigger_CustomType), pollingIntervalInMs: pollingInterval)] CustomType entry,
            ILogger logger)
        {
            logger.LogInformation(IntegrationTestHelpersCopy.GetLogValue(entry));
        }

        [FunctionName("CosmosToRedis")]
        public static void Run([CosmosDBTrigger(
        databaseName: "back",
        containerName: "async",
        Connection = "Endpoint",
        LeaseContainerName = "leases")]IReadOnlyList<ListData> readOnlyList, ILogger log)
        {
            if (readOnlyList == null || readOnlyList.Count <= 0) return;

            //Accessing each entry from readOnlyList
            foreach (ListData inputValues in readOnlyList)
            {
                //Converting one entry into an array format
                RedisValue[] redisValues = Array.ConvertAll(inputValues.value.ToArray(), item => (RedisValue)item);

                //Getting the value of the entry to push into the desired user specified key
                foreach (var entryValue in redisValues)
                {
                    //Push key that is updated by user, with values into the cache
                    cache.ListRightPush(key, entryValue);

                    //Optional log to confirm each item is sent to the cache, optional to keep
                    log.LogInformation("Saved item Azure Redis cache");
                }
            }
        }

        [FunctionName(nameof(ListTriggerAsync))]
        public static async Task ListTriggerAsync(
            [RedisListTrigger(localhostSetting, key)] string listEntry, [CosmosDB(
            Connection = "Endpoint" )]CosmosClient client,
            ILogger logger)
        {
            //Retrieve the database and container from the given client, which accesses the CosmosDB Endpoint
            Container db = client.GetDatabase(databaseName).GetContainer(containerName);

            //Creates query for item inthe container and
            //uses feed iterator to keep track of token when receiving results from query
            IOrderedQueryable<ListData> query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == key)
                .ToFeedIterator();

            //Retrieve collection of items from results and then the first element of the sequence
            var response = await results.ReadNextAsync();
            var item = response.FirstOrDefault(defaultValue: null);

            //Optional logger to display what is being pushed to CosmosDB
            logger.LogInformation("The value added is " + listEntry);

            //If there doesnt exist an entry with this key in CosmosDB, create a new entry
            if (item == null)
            {
                List<string> resultsHolder = new List<string>
                {
                    listEntry
                };

                ListData newEntry = new ListData(id: key, value: resultsHolder);
                await db.UpsertItemAsync(newEntry);
            }

            //If there exists an entry with this key in CosmosDB, add the new values to the existing entry
            else
            {
                List<string> resultsHolder = item.value;

                resultsHolder.Add(listEntry);
                ListData newEntry = new ListData(id: key, value: resultsHolder);
                await db.UpsertItemAsync<ListData>(newEntry);
            }
        }


        [FunctionName(nameof(ListTriggerReadThroughFunc))]
        public static async Task ListTriggerReadThroughFunc(
            [RedisPubSubTrigger(localhostSetting, "__keyevent@0__:keymiss")] string listEntry, [CosmosDB(
            Connection = "Endpoint" )]CosmosClient client,
            ILogger logger)
        {
            //Retrieve the database and container from the given client, which accesses the CosmosDB Endpoint
            Container db = client.GetDatabase(databaseName).GetContainer(containerName);

            //Creates query for item inthe container and
            //uses feed iterator to keep track of token when receiving results from query
            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == listEntry)
                .ToFeedIterator();

            //Retrieve collection of items from results and then the first element of the sequence
            var response = await results.ReadNextAsync();
            var item = response.FirstOrDefault(defaultValue: null);

            //If there doesnt exist an entry with this key in cosmos, no data will be retrieved
            if (item == null) return;

            // If there exists an entry with this key in cosmos, 
            else
            {
                //Optional logger to display the name of the list trying to be retrieved
                logger.LogInformation(listEntry);

                await toCacheAsync(response, item, listEntry);
            }
        }
    }
}

