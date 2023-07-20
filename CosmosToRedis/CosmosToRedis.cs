using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public static class CosmosToRedis
    {
        //Redis Cache primary connection string from local.settings.json
        public const string localhostSetting = "redisLocalhost";
        private static readonly IDatabase cache = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable(localhostSetting)).GetDatabase();


        //CosmosDB Endpoint from local.settings.json
        public const string Endpoint = "Endpoint";

        //Uses the key of the user's choice and should be changed accordingly
        public const string key = "listTest";

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