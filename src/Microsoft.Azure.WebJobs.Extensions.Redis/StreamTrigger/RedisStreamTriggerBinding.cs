﻿using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Protocols;
using Microsoft.Azure.WebJobs.Host.Triggers;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Extensions.Redis
{
    /// <summary>
    /// Trigger Binding, manages and binds context to listener.
    /// </summary>
    internal class RedisStreamTriggerBinding : ITriggerBinding
    {
        private readonly string connectionString;
        private readonly TimeSpan pollingInterval;
        private readonly int messagesPerWorker;
        private readonly string key;
        private readonly int count;
        private readonly bool deleteAfterProcess;
        private readonly Type parameterType;
        private readonly ILogger logger;

        public RedisStreamTriggerBinding(string connectionString, string key, TimeSpan pollingInterval, int messagesPerWorker, int count, bool deleteAfterProcess, Type parameterType, ILogger logger)
        {
            this.connectionString = connectionString;
            this.key = key;
            this.pollingInterval = pollingInterval;
            this.messagesPerWorker = messagesPerWorker;
            this.count = count;
            this.deleteAfterProcess = deleteAfterProcess;
            this.parameterType = parameterType;
            this.logger = logger;
        }

        public Type TriggerValueType => typeof(StreamEntry);
        public IReadOnlyDictionary<string, Type> BindingDataContract => CreateBindingDataContract();

        public Task<ITriggerData> BindAsync(object value, ValueBindingContext context)
        {
            StreamEntry entry = (StreamEntry)value;
            IReadOnlyDictionary<string, object> bindingData = CreateBindingData(entry);
            return Task.FromResult<ITriggerData>(new TriggerData(new StreamEntryValueProvider(entry, parameterType), bindingData));
        }

        public Task<IListener> CreateListenerAsync(ListenerFactoryContext context)
        {
            if (context is null)
            {
                logger?.LogError($"[{nameof(RedisStreamTriggerBinding)}] Provided {nameof(ListenerFactoryContext)} is null.");
                throw new ArgumentNullException(nameof(context));
            }

            return Task.FromResult<IListener>(new RedisStreamListener(context.Descriptor.LogName, connectionString, key, pollingInterval, messagesPerWorker, count, context.Descriptor.Id, deleteAfterProcess, context.Executor, logger));
        }

        public ParameterDescriptor ToParameterDescriptor()
        {
            return new RedisStreamTriggerParameterDescriptor
            {
                Key = key
            };
        }


        internal static IReadOnlyDictionary<string, Type> CreateBindingDataContract()
        {
            return new Dictionary<string, Type>()
            {
                { "Key", typeof(string) },
                { nameof(StreamEntry.Id), typeof(string) },
                { nameof(StreamEntry.Values), typeof(Dictionary<string, string>) },
            };
        }

        internal IReadOnlyDictionary<string, object> CreateBindingData(StreamEntry entry)
        {
            return new Dictionary<string, object>()
            {
                { "Key", key },
                { nameof(StreamEntry.Id), entry.Id.ToString() },
                { nameof(StreamEntry.Values), RedisUtilities.StreamEntryToDictionary(entry) },
            };
        }
    }
}