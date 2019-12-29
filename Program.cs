using System;
using System.IO;
using Confluent.Kafka;

namespace TesteKafka
{
    public class Program
    {
        public static void Main(string[] args)
        {
            DateTime begin = DateTime.Now;

            string path = @"/Users/italosantana/Desktop/TesteKafka/Output.txt";
            
            var conf = new ProducerConfig { BootstrapServers = "localhost:31090", QueueBufferingMaxMessages = 1000000 };

            Action<DeliveryReport<Null, string>> handler = r =>
            {
                using (StreamWriter sw = File.AppendText(path))
                {
                    sw.WriteLine($"Delivered message in {(DateTime.Now - begin).TotalSeconds.ToString()} seconds");
                }
            };

            
            using (var p = new ProducerBuilder<Null, string>(conf).Build())
            {
                Console.WriteLine(begin.ToString("dd/MM/yyyy hh:mm:ss.fff tt"));

                for (int i=0; i<10; ++i)
                {
                    p.Produce("bar", new Message<Null, string> { Value = i.ToString() }, handler);
                }

                p.Flush(TimeSpan.FromSeconds(10)); 
            }
        }
    }
}
