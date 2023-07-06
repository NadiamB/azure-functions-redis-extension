using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.WebJobs;
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

namespace CosmosToRedis
{
    public record ListData
    (
        string id,
        List<string> value
    );

    internal class CosmosToRediscs
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

        [FunctionName("CosmosToRedis")]
        public static void Run([CosmosDBTrigger(
        databaseName: "databasename",
        containerName: "continername",
        Connection = "connectionstringdb",
        LeaseContainerName = "leases")]IReadOnlyList<ListData> input, ILogger log)
        {
            if (input == null || input.Count <= 0) return;

            var cache = redisconnect.GetDatabase();

            Console.WriteLine(input);

            foreach (var value2 in input)
            {
                RedisValue[] redisValues = Array.ConvertAll(value2.value.ToArray(), item => (RedisValue)item);
                foreach (var value in redisValues)
                {
                    cache.ListRightPush("listName", value);
                    log.LogInformation($"Saved item with id {input.Count} in Azure Redis cache");
                }
            }
        }
    }
}