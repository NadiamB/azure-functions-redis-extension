using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    [Collection("RedisTriggerTests")]
    public class RedisListTriggerTestsCopy
    {
        [Fact]
        public async void ListsTrigger_WriteBack()
        {
            string functionName = nameof(RedisListTriggerTestFunctions.ListTriggerAsync);
            //string functionName = nameof(RedisListTriggerTestFunctions.ListTrigger_String);
            RedisValue[] valuesArray = new RedisValue[] { "a", "b" };

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpersCopy.localsettings, RedisListTriggerTestFunctions.localhostSetting)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = IntegrationTestHelpersCopy.StartFunction(functionName, 7071))
                {
                    functionsProcess.OutputDataReceived += IntegrationTestHelpersCopy.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync("listTest", valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(5));

                    await multiplexer.CloseAsync();
                    functionsProcess.Kill();
                };
                var incorrect = counts.Where(pair => pair.Value != 0);
                Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
            }
        }

        [Fact]
        public async void ListsTrigger_WriteBackHeavyLoading()
        {
            string functionName = nameof(RedisListTriggerTestFunctions.ListTriggerAsync);
            //string functionName = nameof(RedisListTriggerTestFunctions.ListTrigger_String);
            RedisValue[] valuesArray = new RedisValue[200];
            for (int i = 0; i < valuesArray.Length; i++)
            {
                valuesArray[i] = "test" + i;
            }

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpersCopy.localsettings, RedisListTriggerTestFunctions.localhostSetting)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = IntegrationTestHelpersCopy.StartFunction(functionName, 7071))
                {
                    functionsProcess.OutputDataReceived += IntegrationTestHelpersCopy.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync("listTest", valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(256));

                    await multiplexer.CloseAsync();
                    functionsProcess.Kill();
                };
                var incorrect = counts.Where(pair => pair.Value != 0);
                Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
            }
        }

        [Fact]
        public async void ListsTrigger_InCosmos()
        {
            CosmosClientBuilder clientBuilder = new CosmosClientBuilder("Endpoint");
            CosmosClient cosmosClient = clientBuilder.Build();

                //feed iterator
                string key = "listTest";

                Container db = cosmosClient.GetDatabase("back").GetContainer("async");
                var query = db.GetItemLinqQueryable<ListData>();
                using FeedIterator<ListData> results = query
                    .Where(p => p.id == "listTest")
                    .ToFeedIterator();

                var response = await results.ReadNextAsync();
                var item = response.FirstOrDefault(defaultValue: null);

                await Task.Delay(TimeSpan.FromSeconds(5));

                Assert.Equal(item.id, key);
        }

        [Fact]
        public async void ListsTrigger_CosmosToRedis()
        {
            string key = "listTest";
            CosmosClientBuilder clientBuilder = new CosmosClientBuilder("Endpoint");
            CosmosClient cosmosClient = clientBuilder.Build();
            string func = "CosmosToRedis";
            bool exists = false;


            using (Process functionsProcess = IntegrationTestHelpersCopy.StartFunction(func, 7071))
            using(ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpersCopy.localsettings, RedisListTriggerTestFunctions.localhostSetting)))
            { 
                await Task.Delay(TimeSpan.FromSeconds(10));
                functionsProcess.Kill();

                exists = await multiplexer.GetDatabase().KeyExistsAsync(key);
            };

            Assert.True(exists);
        }
     

        

        [Fact]
        public async void ListsTrigger_ReadThrough()
        {
            string functionName = nameof(RedisListTriggerTestFunctions.ListTriggerReadThroughFunc);
            string key = "listTest";
            bool exists;
            RedisValue[] valuesArray = new RedisValue[] { "a", "b" };

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpersCopy.localsettings, RedisListTriggerTestFunctions.localhostSetting)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess = IntegrationTestHelpersCopy.StartFunction(functionName, 7071))
                {
                    await multiplexer.GetDatabase().ListRangeAsync(key, 0, -1);

                    await Task.Delay(TimeSpan.FromSeconds(10));

                    exists = await multiplexer.GetDatabase().KeyExistsAsync(key);
                    await multiplexer.CloseAsync();
                    functionsProcess.Kill();
                };
                

                Assert.True(exists);
            }
        
        }
        


        [Fact]
        public async void ListsTrigger_ScaledOutInstances_DoesntDuplicateEvents()
        {
            string functionName = nameof(RedisListTriggerTestFunctions.ListTrigger_String);
            int count = 100;
            RedisValue[] valuesArray = Enumerable.Range(0, count).Select(x => new RedisValue(x.ToString())).ToArray();

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", valuesArray.Length);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpersCopy.localsettings, RedisListTriggerTestFunctions.localhostSetting)))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);

                using (Process functionsProcess1 = IntegrationTestHelpersCopy.StartFunction(functionName, 7071))
                using (Process functionsProcess2 = IntegrationTestHelpersCopy.StartFunction(functionName, 7072))
                using (Process functionsProcess3 = IntegrationTestHelpersCopy.StartFunction(functionName, 7073))
                {
                    functionsProcess1.OutputDataReceived += IntegrationTestHelpersCopy.CounterHandlerCreator(counts);
                    functionsProcess2.OutputDataReceived += IntegrationTestHelpersCopy.CounterHandlerCreator(counts);
                    functionsProcess3.OutputDataReceived += IntegrationTestHelpersCopy.CounterHandlerCreator(counts);

                    await multiplexer.GetDatabase().ListLeftPushAsync(functionName, valuesArray);

                    await Task.Delay(TimeSpan.FromSeconds(count / 5));

                    await multiplexer.CloseAsync();
                    functionsProcess1.Kill();
                    functionsProcess2.Kill();
                    functionsProcess3.Kill();
                };
            }
            var incorrect = counts.Where(pair => pair.Value != 0);
            Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
        }

        [Theory]
        [InlineData(nameof(RedisListTriggerTestFunctions.ListTrigger_String), typeof(string))]
        [InlineData(nameof(RedisListTriggerTestFunctions.ListTrigger_RedisValue), typeof(RedisValue))]
        [InlineData(nameof(RedisListTriggerTestFunctions.ListTrigger_ByteArray), typeof(byte[]))]
        [InlineData(nameof(RedisListTriggerTestFunctions.ListTrigger_CustomType), typeof(CustomType))]
        public async void ListTrigger_TypeConversions_WorkCorrectly(string functionName, Type destinationType)
        {

            ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
            counts.TryAdd($"Executed '{functionName}' (Succeeded", 1);
            counts.TryAdd(destinationType.FullName, 1);

            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(RedisUtilities.ResolveConnectionString(IntegrationTestHelpersCopy.localsettings, RedisListTriggerTestFunctions.localhostSetting)))
            using (Process functionsProcess = IntegrationTestHelpersCopy.StartFunction(functionName, 7071))
            {
                functionsProcess.OutputDataReceived += IntegrationTestHelpersCopy.CounterHandlerCreator(counts);
                ISubscriber subscriber = multiplexer.GetSubscriber();

                await multiplexer.GetDatabase().ListLeftPushAsync(functionName, JsonConvert.SerializeObject(new CustomType() { Field = "feeld", Name = "naim", Random = "ran" }));
                await Task.Delay(TimeSpan.FromSeconds(1));

                await multiplexer.CloseAsync();
                functionsProcess.Kill();
            };
            var incorrect = counts.Where(pair => pair.Value != 0);
            Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
        }
    }
}

