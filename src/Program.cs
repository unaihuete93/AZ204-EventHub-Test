using System;
using System.Text;
using System.Threading.Tasks;
//dotnet add package Azure.Messaging.EventHubs
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs.Consumer;

namespace EventHubProgram
{
    class Program
    {
       //read configuration from env variable (GitHub Secrets)
        private static string connectionString = Environment.GetEnvironmentVariable("EVENTHUB_CONNECTION_STRING");

        private const string eventHubName = "evenhub-1p";
        private static EventHubProducerClient producerClient;
        private static EventHubConsumerClient consumerClient;

        static async Task Main(string[] args)
        {
            //Create a loop with exit option
            while (true)
            {
                Console.WriteLine("Welcome to the Azure Event Hub program!");
                Console.WriteLine("Please select an option:");
                Console.WriteLine("1. Exit");
                Console.WriteLine("2. Receive events from the event hub");
                Console.WriteLine("3. Send events to the event hub");
                Console.WriteLine("4. Send events to 10 partitions of the event hub");
                Console.WriteLine("5. Read from all partition with details");       
                int option = int.Parse(Console.ReadLine());
                switch (option)
                {
                    case 1:
                        return;
                    case 2:
                        await ReceiveEventsAsync();
                        break;
                    case 3:
                        await SendEventsAsync();
                        break;
                    case 4:
                        await SendEventsToPartitionsAsync();
                        break;
                    case 5:
                        await ReadFromPartitionsAsync();
                        break;
                    default:
                        Console.WriteLine("Invalid option. Please try again.");
                        break;
                }
            }
        }

        static async Task SendEventsAsync()
        {
            Console.WriteLine("Enter the number of events to send:");
            int numberOfEvents = int.Parse(Console.ReadLine());
            producerClient = new EventHubProducerClient(connectionString, eventHubName);
            for (int i = 0; i < numberOfEvents; i++)
            {
                string message = $"Event {i + 1}";
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message)));
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"Sent event: {message}");
            }
            await producerClient.DisposeAsync();
            Console.WriteLine($"Sent {numberOfEvents} events successfully.");
        }

        static async Task ReceiveEventsAsync()
        {
            //This example makes use of the ReadEvents method of the EventHubConsumerClient, which allows it to see events from all partitions of an Event Hub. While this is convenient to use for exploration, we strongly recommend not using it for production scenarios. ReadEvents does not guarantee fairness amongst the partitions during iteration; each of the partitions compete to publish events to be read.
            Console.WriteLine("Enter the number of events to receive:");
            int numberOfEvents = int.Parse(Console.ReadLine());
            consumerClient = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, connectionString, eventHubName);
            int receivedEvents = 0;
            await foreach (PartitionEvent receivedEvent in consumerClient.ReadEventsAsync())
            {
                Console.WriteLine($"Received event: {Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray())}");
                if (++receivedEvents >= numberOfEvents)
                {
                    break;
                }
            }
            await consumerClient.DisposeAsync();
            Console.WriteLine($"Received {receivedEvents} events successfully.");
        }

        //Send Event to 10 different partitions of the event hub
        static async Task SendEventsToPartitionsAsync()
        {
            int numberOfEvents = 10;
            
            string[] partitionKeys = { "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10" };
            producerClient = new EventHubProducerClient(connectionString, "eventhub-10p");
            for (int i = 0; i < numberOfEvents; i++)
            {
                string message = $"Event {i + 1}";
                string partitionKey = partitionKeys[i % partitionKeys.Length];
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync(new CreateBatchOptions { PartitionKey = partitionKey });
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message)));
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"Sent event: {message} to partition with key {partitionKey}");
            }
            await producerClient.DisposeAsync();
            Console.WriteLine($"Sent {numberOfEvents} events to partitions successfully.");
        }

        //Read from a all partitions
        static async Task ReadFromPartitionsAsync()
        {
            Console.WriteLine("Reading events from the event hub...");
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            await using var consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, "eventhub-10p");
            try
            {
                using CancellationTokenSource cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(45));

                int eventsRead = 0;
                int maximumEvents = 3;

                await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsAsync(cancellationSource.Token))
                {
                    string readFromPartition = partitionEvent.Partition.PartitionId;
                    byte[] eventBodyBytes = partitionEvent.Data.EventBody.ToArray();
                    string eventBody = Encoding.UTF8.GetString(eventBodyBytes);
                    //console output with event body
                    Console.WriteLine($"Event:  { eventBody } from { readFromPartition }");
                    eventsRead++;

                    if (eventsRead >= maximumEvents)
                    {
                        break;
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
            }
            finally
            {
                await consumerClient.CloseAsync();
            }
        
        }   
         
    }
}
