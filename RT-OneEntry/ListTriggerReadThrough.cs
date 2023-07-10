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

namespace RT_OneEntry
{
    public record ListData
    (
        string id,
        List<string> value
    );
    public static class ListTriggerReadThrough
    {
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();

        [FunctionName(nameof(ListTriggerReadThroughFunc))]
        public static async Task ListTriggerReadThroughFunc(
            [RedisPubSubTrigger(localhostSetting, "__keyevent@0__:keymiss")] string listEntry, [CosmosDB(
                            databaseName: "dbname",
                            containerName: "containername",
                            Connection = "endpoint" )]CosmosClient input,
            ILogger logger)
        {
            Container db = input.GetDatabase("back").GetContainer("async");
            if(await cache.KeyExistsAsync(listEntry) == true){
                Console.WriteLine("This entry exists in the the cache.");
            }

            //else, go to cosmos and bring to redis
            else{
                Console.WriteLine("This entry does not exist in the the cache.");
                var query = db.GetItemLinqQueryable<ListData>();
                using FeedIterator<ListData> results = query
                    .Where(p => p.id == "listTest")
                    .ToFeedIterator();
                var response = await results.ReadNextAsync();
                var item = response.FirstOrDefault(defaultValue: null);

                //if there doesnt exist an entry with this key in cosmos, 
                if (item == null){
                    Console.WriteLine("This key does not exist in CosmosDB either.");
                }

                //else, bring from cosmos to redis
                else{
                    Console.WriteLine("The key is in CosmosDB ");
                    logger.LogInformation(listEntry);
                    //get the amount of elements in cosmos associated with the key, so you can access each item
                    var item2 = response.Take(response.Count);
                    if (item2 == null) return;
                    foreach (var val in item2){
                        RedisValue[] redisValues = Array.ConvertAll(val.value.ToArray(), item => (RedisValue)item);
                        foreach (var value2 in redisValues)
                        {
                            await cache.ListRightPushAsync("listTest", value2);
                            Console.WriteLine($"Saved item");
                        }
                    }
                }
            }
        }
    }
}

