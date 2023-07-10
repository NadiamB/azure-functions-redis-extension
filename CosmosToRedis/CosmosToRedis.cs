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

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public record ListData
    (
        string id,
        List<string> value
    );

    public static class CosmosToRedis
    {

        //redis connection string
        static ConnectionMultiplexer redisconnect = ConnectionMultiplexer.Connect("PrimaryConnectionString");
        static IDatabase cache = redisconnect.GetDatabase();
        public const string localhostSetting = "redisLocalhost";
        public const string Endpoint = "Endpoint";

        [FunctionName("CosmosToRedis")]
        public static void Run([CosmosDBTrigger(
        databaseName: "databaseName",
        containerName: "containerName",
        Connection = "Endpoint",
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
                    cache.ListRightPush("listTest", value);
                    log.LogInformation($"Saved item with id {input.Count} in Azure Redis cache");
                }
            }
        }
    }
}