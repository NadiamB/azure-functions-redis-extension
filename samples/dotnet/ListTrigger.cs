using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Collections;
using System.ComponentModel;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public static class ListTrigger
    {
        //redis connection string
        public const string localhostSetting = "redisLocalhost";

        //connecting to CosmosDB
        //primary connection string
        static readonly string Endpoint = "Endpoint";
        static readonly CosmosClient cc = new CosmosClient(Endpoint);
        static readonly Cosmos.Container db = cc.GetDatabase("Database Name").GetContainer("Container Name");

        [FunctionName(nameof(ListTriggerAsync))]
        public static async Task ListTriggerAsync(
            [RedisListTrigger(localhostSetting, "listTest")] string entry,
            ILogger logger)
        {
            logger.LogInformation(entry);
            string value = entry.ToString();
            Guid id = Guid.NewGuid();

            Hashtable pair = new Hashtable()
            {
                { "id", id },  // Unique identifier for the document
                { "key", "listTest" },
                {"value", entry }
            };

            await db.CreateItemAsync(pair);
        }
    }
}