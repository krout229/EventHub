using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
namespace EventReceiver
{
    class Program
    {
        private const string ehubNamespaceConnectionString = "Endpoint=sb://eventsender123.servicebus.windows.net/;SharedAccessKeyName=eventListen;SharedAccessKey=0+OWceuaMPAVVUKquTpDGsz6AQ6K47o7zlYQaHBw8PY=;EntityPath=eventsender";
        private const string eventHubName = "eventsender";
        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=eventstorage123;AccountKey=2YQ80jI49cV8EuQs5wpe9BEV7eQJFS6vEHNgJGf1lduCFmm9QBA3Ao/0rWM4IsAasyJfkRP1z4Qu+AStF0Mx7A==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "demoblob";
        static BlobContainerClient storageClient;

                
        static EventProcessorClient processor;
        static async Task Main()
        {
            // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 30 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(30));

            // Stop the processing
            await processor.StopProcessingAsync();
        }
        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}