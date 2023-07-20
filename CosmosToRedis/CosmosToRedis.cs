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
        //Redis Ccche primary connection string from local.settings.json
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();


        //CosmosDB Endpoint from local.settings.json
        public const string Endpoint = "Endpoint";

        [FunctionName("CosmosToRedis")]
        public static void Run([CosmosDBTrigger(
        databaseName: "databaseName",
        containerName: "containerName",
        Connection = "Endpoint",
        LeaseContainerName = "leases")]IReadOnlyList<ListData> readOnlyList, ILogger log)
        {
            if (readOnlyList == null || readOnlyList.Count <= 0) return;

            //Accessing each entry from readOnlyList
            foreach (ListData inputValues in readOnlyList)
            {
                //Converting one entry into an array format
                RedisValue[] redisValues = Array.ConvertAll(inputValues.value.ToArray(), item => (RedisValue)item);

                //Getting the value of the entry to push into the desired user specified key
                foreach (var entryValue in redisValues)
                {
                    //Push  key of "listTest", but can be updated to a desired key, with values into the cache
                    cache.ListRightPush("listTest", entryValue);

                    //Optional log to confirm each item is sent to the cache, optional to keep
                    log.LogInformation("Saved item Azure Redis cache");
                }
            }
        }
    }
}