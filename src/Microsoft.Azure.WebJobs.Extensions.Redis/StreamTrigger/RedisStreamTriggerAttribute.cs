﻿using Microsoft.Azure.WebJobs.Description;
using System;

namespace Microsoft.Azure.WebJobs.Extensions.Redis
{
    /// <summary>
    /// Redis streams trigger binding attributes.
    /// </summary>
    [Binding]
    [AttributeUsage(AttributeTargets.Parameter)]
    public class RedisStreamTriggerAttribute : RedisPollingTriggerBaseAttribute
    {
        /// <summary>
        /// Initializes a new <see cref="RedisStreamTriggerAttribute"/>.
        /// </summary>
        /// <param name="connectionStringSetting">Redis connection string setting.</param>
        /// <param name="key">Key to read from.</param>
        /// <param name="pollingIntervalInMs">How often to poll Redis in milliseconds. Default: 1000</param>
        /// <param name="messagesPerWorker">The number of messages each functions instance is expected to handle. Default: 100</param>
        /// <param name="count">Number of entries to pull from a Redis stream at one time. Default: 10</param>
        /// <param name="deleteAfterProcess">Decides if the function will delete the stream entries after processing. Default: false</param>
        public RedisStreamTriggerAttribute(string connectionStringSetting, string key, int pollingIntervalInMs = 1000, int messagesPerWorker = 100, int count = 10, bool deleteAfterProcess = false)
            : base(connectionStringSetting, key, pollingIntervalInMs, messagesPerWorker, count)
        {
            DeleteAfterProcess = deleteAfterProcess;
        }

        /// <summary>
        /// Decides if the function will delete the stream entries after processing.
        /// </summary>
        public bool DeleteAfterProcess { get; }
    }
}