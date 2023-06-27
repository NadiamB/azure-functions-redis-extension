﻿using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Redis;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Azure.Core.HttpHeader;

namespace WriteThrough
{
    internal class WriteThrough
    {
        public const string localhostSetting = "redisLocalhost";

        //connecting to CosmosDB
        //primary connection string from your database in "Account Endpoint here"
        static readonly string Endpoint = "AccountEndpoint=https://cosbackup.documents.azure.com:443/;AccountKey=yoctt2Rr0r3Y9hUPPIBO99NE36J877WDWgThLuI7zJdnO2rdNnKB0v5UuCQLAGxfdpBGPvaFafdCACDboQdAAw==;";
        static readonly CosmosClient cc = new CosmosClient(Endpoint);
        static readonly Microsoft.Azure.Cosmos.Container db = cc.GetDatabase("backup").GetContainer("backcos");

        [FunctionName(nameof(ListTrigger))]
        public static async Task ListTrigger(
            [RedisListTrigger(localhostSetting, "listTest")] string entry,
            ILogger logger)
        {
            logger.LogInformation(entry);
            string value = entry.ToString();
            Guid id = Guid.NewGuid();

            Hashtable pair = new Hashtable()
            {
                { "id", id },  // Unique identifier for the document
                { "key1", entry }
            };

            ItemResponse<Hashtable> response = db.CreateItemAsync(pair).Result;

            Hashtable newpair = response.Resource;
        }

    }
}
