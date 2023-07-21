using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Collections.Generic;
using StackExchange.Redis;
using System.Linq;
using Microsoft.Azure.Cosmos.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public static class WriteBack
    {
        //Redis Cache primary connection string from local.settings.json
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        //CosmosDB Endpoint from local.settings.json
        public const string Endpoint = "Endpoint";

        //CosmosDB database name and container name declared here
        public const string databaseName = "databaseName";
        public const string containerName = "containerName";

        //Uses the key of the user's choice and should be changed accordingly
        public const string key = "listTest";

       [FunctionName(nameof(ListTriggerAsync))]
        public static async Task ListTriggerAsync(
            [RedisListTrigger(localhostSetting, key)] string listEntry, [CosmosDB(
            Connection = "Endpoint" )]CosmosClient client,
            ILogger logger)
        {
            //Retrieve the database and container from the given client, which accesses the CosmosDB Endpoint
            Container db = client.GetDatabase(databaseName).GetContainer(containerName);

            ///ListData holding the entry to be uploaded and List<string> holding what will be added to the newEntry
            ListData newEntry;
            List<string> resultsHolder;

            //Creates query for item inthe container and
            //uses feed iterator to keep track of token when receiving results from query
            IOrderedQueryable<ListData> query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == key)
                .ToFeedIterator();

            //Retrieve collection of items from results and then the first element of the sequence
            FeedResponse<ListData> response = await results.ReadNextAsync();
            ListData item = response.FirstOrDefault(defaultValue: null);

            //Optional logger to display what is being pushed to CosmosDB
            logger.LogInformation("The value added is " + listEntry);

            resultsHolder = new List<string>();

            //Create an entry if the key doesn't exist in CosmosDB or add to it if there is an existing entry
            resultsHolder = item?.value ?? new List<string>();

            resultsHolder.Add(listEntry);
            newEntry = new ListData(id: key, value: resultsHolder);
            await db.UpsertItemAsync<ListData>(newEntry);
        }

    }
}
