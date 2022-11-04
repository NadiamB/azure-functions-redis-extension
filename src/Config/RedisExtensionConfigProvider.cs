﻿using System;
using System.Text.Json;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.WebJobs.Host.Config;
using Microsoft.Extensions.Configuration;

namespace Microsoft.Azure.WebJobs.Extensions.Redis
{

    [Extension("Redis")]
    public class RedisExtensionConfigProvider : IExtensionConfigProvider
    {
        private readonly IConfiguration configuration;
        public RedisExtensionConfigProvider(IConfiguration configuration) 
        {
            this.configuration = configuration;
        }

        /// <summary>
        /// Initializes binding to trigger provider via binding rule.
        /// </summary>
        public void Initialize(ExtensionConfigContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

#pragma warning disable CS0618
            FluentBindingRule<RedisTriggerAttribute> rule = context.AddBindingRule<RedisTriggerAttribute>();
#pragma warning restore CS0618
            rule.BindToTrigger<RedisMessageModel>(new RedisTriggerBindingProvider(configuration));
            rule.AddConverter<RedisMessageModel, string>(args => JsonSerializer.Serialize(args));
        }
    }
}
