using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Redis;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Design;
using System.Drawing;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;
using Container = Microsoft.Azure.Cosmos.Container;

namespace RT_OneEntry
{
    public record ListData
    (
        string id,
        List<string> value
    );
    internal class ListTriggerReadThrough
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

        [FunctionName(nameof(ListTriggerReadThroughFunc))]
        public static async Task ListTriggerReadThroughFunc(
            [RedisPubSubTrigger(localhostSetting, "__keyevent@0__:keymiss")] string entry, [CosmosDB(
                            databaseName: "databasename",
                            containerName: "containername",
                            Connection = "connectiontodb" )]CosmosClient input,
            ILogger logger)
        {
            bool keyInside = cache.KeyExists(entry);
            if(keyInside == true){
                Console.WriteLine("This entry exists in the the cache.");
                return;
            }
            //else, go to cosmos and bring to redis
            else{
                Console.WriteLine("This entry does not exist in the the cache.");
                var query = db.GetItemLinqQueryable<ListData>();
                using FeedIterator<ListData> f = query
                    .Where(p => p.id == "listName")
                    .ToFeedIterator();
                //if there doesnt exist an entry with this key in cosmos, 
                var response = await f.ReadNextAsync();
                var item = response.FirstOrDefault(defaultValue: null);
                if (item == null){
                    Console.WriteLine("This key does not exist in CosmosDB either.");
                    return;
                }
                //else, bring from cosmos to redis
                else{
                    Console.WriteLine("The key is in CosmosDB ");
                    logger.LogInformation(entry);
                    string value = entry.ToString();

                    var cache = redisconnect.GetDatabase();
                    if (response.StatusCode == System.Net.HttpStatusCode.OK)
                    {
                        var item2 = response.Take(response.Count);
                        if (item2 == null) return;
                        foreach (var val in item2){
                            RedisValue[] redisValues = Array.ConvertAll(val.value.ToArray(), item => (RedisValue)item);
                            foreach (var value2 in redisValues)
                            {
                                cache.ListRightPush("listName", value2);
                                Console.WriteLine($"Saved item");
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("This key does not exist in cosmos.");
                    }
                }
            }
            return;
        }
    }
}

