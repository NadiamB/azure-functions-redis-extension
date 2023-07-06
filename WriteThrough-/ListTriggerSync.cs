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
        static ConnectionMultiplexer redisconnect = ConnectionMultiplexer.Connect("RediscachePrimaryString");
        static IDatabase cache = redisconnect.GetDatabase();
        public const string localhostSetting = "redisLocalhost";

        //connecting to CosmosDB
        //primary connection string
        static readonly string Endpoint = "Endpoint";
        static readonly CosmosClient cc = new CosmosClient(Endpoint);
        static readonly Microsoft.Azure.Cosmos.Container db = cc.GetDatabase("databasename").GetContainer("containername");

        [FunctionName(nameof(ListTrigger))]
        public static void ListTrigger(
            [RedisListTrigger(localhostSetting, "listName")] string entry,
            ILogger logger)
        {
            var query = db.GetItemLinqQueryable<ListData>();
            using FeedIterator<ListData> f = query
                .Where(p => p.id == "listName")
                .ToFeedIterator();
            //if there doesnt exist an entry with this key in cosmos

            //.Result blocks the execution until the result is available
            //this can cause deadlock so thats why asynch is preferred
            var response = f.ReadNextAsync().Result;
            var item = response.FirstOrDefault(defaultValue: null);
            if (item == null)
            {
                Console.WriteLine("Query null");
                logger.LogInformation(entry);
                string value = entry.ToString();
                //Guid id = Guid.NewGuid();
                List<string> temp = new List<string>
                {
                    entry
                };

                ListData pair = new ListData(id: "listName", value: temp);

                db.UpsertItemAsync(pair);
            }

            else
            {
                Console.WriteLine("Querry not null ");
                logger.LogInformation(entry);
                string value = entry.ToString();

                //get the current values associated with listTest in CosmosDB
                //ListData toDoActivity = await db.ReadItemAsync<ListData>(item.id, new PartitionKey("/id"));

                List<string> temp2 = item.value;
                Console.WriteLine("Current vals: " + item.value);

                temp2.Add(entry);
                ListData pair = new ListData(id: "listName", value: temp2);
                List<PatchOperation> patchOperations = new List<PatchOperation>()
                {
                    PatchOperation.Replace("/value", temp2),
                };
                Console.WriteLine("Before last line ");

                ItemResponse<ListData> item2 =  db.UpsertItemAsync<ListData>(pair).Result;
            }
        }

    }
}