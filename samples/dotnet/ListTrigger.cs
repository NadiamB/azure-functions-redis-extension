using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using System.Collections;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Samples
{
    public static class ListTrigger
    {
        public const string localhostSetting = "redisLocalhost";

        //connecting to CosmosDB
        //primary connection string
        static readonly string Endpoint = "AccountEndpoint=https://cosbackup.documents.azure.com:443/;AccountKey=yoctt2Rr0r3Y9hUPPIBO99NE36J877WDWgThLuI7zJdnO2rdNnKB0v5UuCQLAGxfdpBGPvaFafdCACDboQdAAw==;";
        static readonly CosmosClient cc = new CosmosClient(Endpoint);
        static readonly Container db = cc.GetDatabase("backup").GetContainer("backcos");

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
