using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.IO;
using static System.IO.Path;
using static System.Environment;
using static System.Console;
using WSDemo_1;
using Confluent.Kafka;


namespace WSDemo_1
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                
                while (IsFileThere())
                {
                    Process();
                    await Task.Delay(10000, stoppingToken);
                }
                WriteLine("A file is not found...  I will wait for one to show up");
                await Task.Delay(10000, stoppingToken);
            }
        }

        public static void Process()
        {
            // step 1 log receipt
            WriteLine("A file has been found!");
            // step 2 create output file
            CloudEvent mycloudevent = CreateCloudEvent();
            
            // step 3 create file and serialize cloud event and
            // send to Out folder
            var stream = CreateAndSerializeJSON(mycloudevent);
            
            // step 4 produce stream to kafka
            SendToKafka(stream);
            
        }
        
        public static void SendToKafka(string stream)
        {
            string brokerList = "127.0.0.1:9092";
            string topicName = "test.topic";

            var config = new ProducerConfig {BootstrapServers = brokerList };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                    try
                    {
                        var deliveryReport = producer.ProduceAsync(topicName, new Message<string, string> { Key = "test", Value = stream });
                        producer.Flush();
                                               
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                
            }
        
        }
        
        public static string CreateAndSerializeJSON(CloudEvent myevent)
        {
            string jsonPath = Combine("Out/", "book.json");

            using (StreamWriter jsonStream = File.CreateText(jsonPath))
            {
                var jss = new Newtonsoft.Json.JsonSerializer();
                jss.Serialize(jsonStream, myevent);
            }

            //WriteLine();
            //WriteLine("Written {0:N0} bytes of JSON to: {1}",
            //arg0: new FileInfo(jsonPath).Length,
            //arg1: jsonPath);

            // Display the serialized object graph
            //WriteLine(File.ReadAllText(jsonPath));
            string returnStream = File.ReadAllText(jsonPath);

            return returnStream;

        }
        public static CloudEvent CreateCloudEvent()
        {
            var cloudevent = new CloudEvent
            {
                id = "12345",
                source = "manual",
                specversion = "1.0",
                type = "test.topic",
                subject = "test.topic.test"   
            };

            return cloudevent;
        }
        
        public static bool IsFileThere()
        {
            
            // for more information about call below see - https://dotnetfiddle.net/RNYV4K
            if (Directory.EnumerateFiles("In/", "*.json").Any())
            {
                return true;
            }    
            else
            {
                return false;
            }
        }
    }
}
