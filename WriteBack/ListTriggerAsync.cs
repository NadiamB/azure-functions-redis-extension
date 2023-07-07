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

namespace WriteBack
{
    public record ListData
    (
        string id,
        List<string> value
    );
    internal class WriteBack
    {
        //redis connection string
        static ConnectionMultiplexer redisconnect = ConnectionMultiplexer.Connect("RediscachePrimaryString");
        static IDatabase cache = redisconnect.GetDatabase();
        public const string localhostSetting = "redisLocalhost";

        //connecting to CosmosDB
        //primary connection string
        static readonly string Endpoint = "Endpoint";
        static readonly CosmosClient cc = new CosmosClient(Endpoint);
        static readonly Microsoft.Azure.Cosmos.Container db = cc.GetDatabase("databasename").GetContainer("containername");

        [FunctionName(nameof(ListTriggerAsync))]
        public static async Task ListTriggerAsync(
            [RedisListTrigger(localhostSetting, "listName")] string entry,
            ILogger logger)
        {
            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> f = query
                .Where(p => p.id == "listName")
                .ToFeedIterator();

            var response = await f.ReadNextAsync();
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

                ListData pair = new ListData(id: "listName", value: temp);

                await db.UpsertItemAsync(pair);
            }
            else
            {
                logger.LogInformation(entry);
                string value = entry.ToString();

                List<string> temp2 = item.value;

                temp2.Add(entry);
                ListData pair = new ListData(id: "listName", value: temp2);

                ItemResponse<ListData> item2 = await db.UpsertItemAsync<ListData>(pair);
            }
        }
    }
}
