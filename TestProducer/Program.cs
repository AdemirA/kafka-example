using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;

namespace TestProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("[Producer]");

            var bootstrapServers = GetConfiguration().GetSection("BootstrapServers").Value;
            var topicName = GetConfiguration().GetSection("TopicName").Value;

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers, SecurityProtocol = SecurityProtocol.Ssl }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 3 } 
                    });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }

            var config = new ProducerConfig { 
                BootstrapServers = bootstrapServers, 
                SecurityProtocol = SecurityProtocol.Ssl,
                Acks = Acks.Leader // garante q foi gravado no leader / all => garante que foi gravado nas replicas
            };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                while (true)
                {
                    Console.Write("Send message: ");
                    var message = Console.ReadLine();

                    try
                    {
                        var dr = await p.ProduceAsync(topicName, new Message<Null, string> { Value = message });
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }
            }
        }

        static IConfigurationRoot _configuration;
        static IConfigurationRoot GetConfiguration()
        {
            if (_configuration == null)
            {
                var builder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

                _configuration = builder.Build();
            }

            return _configuration;
        }
    }
}
