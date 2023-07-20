using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Drawing;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public record ListData
    (
        string id,
        List<string> value
    );
    public static class ListTriggerReadThrough
    {
        //Redis Cache primary connection string from local.settings.json
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        //CosmosDB database name and container name declared here
        public const string databaseName = "databaseName";
        public const string containerName = "containerName";

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
            else {
                //Optional logger to display the name of the list trying to be retrieved
                logger.LogInformation(listEntry);

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
        }
    }
}

