using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Collections;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
class Producer {
  public static ArrayList getFiles(string directoryname) {
    ArrayList filescontents = new ArrayList();
    string[] filePaths = Directory.GetFiles(directoryname, "*.*", SearchOption.AllDirectories);
    foreach(string filePath in filePaths) {
      filescontents.Add(System.IO.File.ReadAllText(filePath));
    }
    return filescontents;
  }
  static void Main(string[] args) {
    if (args.Length != 1) {
      Console.WriteLine("Please provide the configuration file paths as a command line argument");
    }
    var pConfig = new ProducerConfig {
      BootstrapServers = "kafka-odspoc.westeurope.azurecontainer.io:9092", LingerMs = 50, BatchNumMessages = 2000, BatchSize = 1000000, CompressionType = CompressionType.Gzip,
    };
    string[] topic;
    topic = new [] {
      "dhlpocboundstreamp10",
      "dhlpocboundstreamp10",
      "dhlpocboundstreamp10"
    };
    Console.WriteLine("Start File prep");
    ArrayList filescontents = getFiles("C:\\xml\\raw\\dstid_1");
    Console.WriteLine("End File prep # Loaded:"
      filescontents.Count);
    Console.WriteLine("Start Sending");
    using(var producer2 = new ProducerBuilder < string, string > (pConfig).Build()) {
      var numProduced = 0;
      int rounds = 40;
      int numMessages = 100000;
      for (int i = 0; i < rounds; i) {
        DateTime start = DateTime.Now;
        Parallel.For(0, numMessages, i => {
          Random rnd = new Random();
          var filecontent = filescontents[rnd.Next(filescontents.Count)];
          var fileKey = System.Guid.NewGuid().ToString();
          var topic2 = topic[rnd.Next(topic.Length)];
          producer2.Produce(topic2, new Message < string, string > {
            Key = fileKey,
            Value = (string) filecontent
          }, (deliveryReport) => {
            if (deliveryReport.Error.Code != ErrorCode.NoError) {
              Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
            } else {
              numProduced = 1;
            }
          });
        });
        var timeItTook1 = (DateTime.Now - start).TotalSeconds;
        Console.WriteLine($"Produced {numProduced} messages in {timeItTook1} Seconds. That are {numProduced / timeItTook1} Messages per Second");
        producer2.Flush(TimeSpan.FromSeconds(20));
        Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        var timeItTook = (DateTime.Now - start).TotalSeconds;
        Console.WriteLine($"Produced {numProduced} messages in {timeItTook} Seconds. That are {numProduced / timeItTook} Messages per Second");
        numProduced = 0;
      }
    }
    Console.WriteLine("Done");
  }
}
