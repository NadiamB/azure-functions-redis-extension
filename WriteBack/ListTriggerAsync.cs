using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Collections;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Redis;
using System.Collections.Generic;
using StackExchange.Redis;
using System.Linq;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public record ListData
    (
        string id,
        List<string> value
    );
    public static class WriteBack
    {
        //redis connection string
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        //connecting to CosmosDB
        public const string Endpoint = "Endpoint";

        [FunctionName(nameof(ListTriggerAsync))]
        public static async Task ListTriggerAsync(
            [RedisListTrigger(localhostSetting, "listTest")] string listEntry, [CosmosDB(
                            databaseName: "databaseName",
                            containerName: "containerName",
                            Connection = "Endpoint" )]CosmosClient input,
            ILogger logger)
        {
            Container db = input.GetDatabase("databaseName").GetContainer("containerName");
            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> results = query
                .Where(p => p.id == "listTest")
                .ToFeedIterator();

            var response = await results.ReadNextAsync();
            var item = response.FirstOrDefault(defaultValue: null);
            //if there doesnt exist an entry with this key in cosmos
            if (item == null)
            {
                logger.LogInformation(listEntry);
                string value = listEntry.ToString();
  
                List<string> temp = new List<string>
                {
                    listEntry
                };

                ListData pair = new ListData(id: "listTest", value: temp);

                await db.UpsertItemAsync(pair);
            }
            else
            {
                logger.LogInformation(listEntry);
                string value = listEntry.ToString();

                List<string> resultsHolder = item.value;

                resultsHolder.Add(listEntry);
                ListData pair = new ListData(id: "listTest", value: resultsHolder);

                ItemResponse<ListData> item2 = await db.UpsertItemAsync<ListData>(pair);
            }
        }

    }
}
