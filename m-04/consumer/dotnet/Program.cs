using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace consumer_net {
    class Program {
        static void Main (string[] args) {
            Console.WriteLine ("Starting Consumer!");
            var config = new Dictionary<string, object> { 
                  { "group.id", "dotnet-consumer-group" },
                  { "bootstrap.servers", "kafka-1:9092" },
                  { "auto.commit.interval.ms", 5000 },
                  { "auto.offset.reset", "earliest" }
                };

            var deserializer = new StringDeserializer (Encoding.UTF8);
            using (var consumer = new Consumer<string, string> (config, deserializer, deserializer)) {
                consumer.OnMessage += (_, msg) => 
                  Console.WriteLine ($"Read ('{msg.Key}', '{msg.Value}') from: {msg.TopicPartitionOffset}");

                consumer.OnError += (_, error) => 
                  Console.WriteLine ($"Error: {error}");

                consumer.OnConsumeError += (_, msg) => 
                  Console.WriteLine ($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                consumer.Subscribe ("hello_world_topic");

                while (true) {
                    consumer.Poll (TimeSpan.FromMilliseconds (100));
                }
            }
        }
    }
}
