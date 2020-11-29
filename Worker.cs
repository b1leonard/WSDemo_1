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
using System.Text.Json;
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
            
            // step 3 create file, serialize cloud event and send to Out folder
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
        // serialize and deserialize example - https://dotnetfiddle.net/RNYV4K
        {
            string jsonPath = Combine("Out/", "book.json");

            using (StreamWriter jsonStream = File.CreateText(jsonPath))
            {
                var jss = new Newtonsoft.Json.JsonSerializer();
                jss.Serialize(jsonStream, myevent);
            }

            string returnStream = File.ReadAllText(jsonPath);

            return returnStream;

        }
        public static CloudEvent CreateCloudEvent()
        {
            string dir = "In/";
            IEnumerable<string> files = Directory.EnumerateFiles(dir, "*.json");
            CloudEvent myCloudEvent = new CloudEvent();

            foreach (string file in files)
            {
                var s = ReadAllText(file);
		        Console.WriteLine(s);
		
		        var stream = JsonSerializer.Deserialize<CloudEvent>(s);

                myCloudEvent = stream;
            }
                        
           return myCloudEvent;

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

        public static string ReadAllText(string path)
	    {
		    return File.ReadAllText(path);
	    }
    }
}