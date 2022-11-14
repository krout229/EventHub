﻿using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace EventHubSender
{
    class Program
    {
        // connection string to the Event Hubs namespace
        private const string connectionString = "Endpoint=sb://eventsender123.servicebus.windows.net/;SharedAccessKeyName=eventSender;SharedAccessKey=EjigY1NlzbWqW2EKxxjEnV+d3liNwPTTNa555YVF2cs=;EntityPath=eventsender";

        // name of the event hub
        private const string eventHubName = "eventsender";

        // number of events to be sent to the event hub
        private const int numOfEvents=3;
        static EventHubProducerClient producerClient;
        static async Task Main()
        {
            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            for (int i = 1; i <= numOfEvents; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {numOfEvents} events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }
       
    }
}
