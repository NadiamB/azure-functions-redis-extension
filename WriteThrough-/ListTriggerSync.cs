using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Collections;
using System.ComponentModel;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Redis;
using System.Collections.Generic;
using StackExchange.Redis;
using System.Linq;
using Microsoft.Azure.Cosmos.Linq;
using Container = Microsoft.Azure.Cosmos.Container;

namespace WriteThrough
{
    public record ListData
    (
        string id,
        List<string> value
    );
    internal class WriteThrough
    {
        //redis connection string
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        //connecting to CosmosDB
        public const string Endpoint = "Endpoint";

        [FunctionName(nameof(ListTrigger))]
        public static void ListTrigger(
            [RedisListTrigger(localhostSetting, "listTest")] string entry, [CosmosDB(
                            databaseName: "back",
                            containerName: "async",
                            Connection = "Endpoint" )]CosmosClient input,
            ILogger logger)
        {
            Container db = input.GetDatabase("back").GetContainer("async");
            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> f = query
                .Where(p => p.id == "listTest")
                .ToFeedIterator();
            
            //.Result blocks the execution until the result is available, /this can cause deadlock so thats why asynch is preferred
            var response = f.ReadNextAsync().Result;
            var item = response.FirstOrDefault(defaultValue: null);

            //if there doesnt exist an entry with this key in cosmos
            if (item == null)
            {
                logger.LogInformation(entry);
                string value = entry.ToString();
                List<string> temp = new List<string>
                {
                    entry
                };

                ListData pair = new ListData(id: "listTest", value: temp);
                db.UpsertItemAsync(pair);
            }
            else
            {
                logger.LogInformation(entry);
                string value = entry.ToString();

                List<string> temp = item.value;

                temp.Add(entry);
                ListData pair = new ListData(id: "listTest", value: temp);

                ItemResponse<ListData> item2 =  db.UpsertItemAsync<ListData>(pair).Result;
            }
        }

    }
}