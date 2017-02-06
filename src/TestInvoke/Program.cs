using System;
using Lykke.Integration.AzureQueueAndBlobs;
using TestInvoke.Publisher;
using TestInvoke.Subscriber;

namespace TestInvoke
{
    public class Program
    {

        public static void Main(string[] args)
        {
            var settings = new AzureQueueAndBlobIntegrationSettings
            {
                ConnectionString = "UseDevelopmentStorage=true",
                BlobContainer = "emails",
                QueueName = "emails"
            };


            HowToPublish.Example(settings).Wait();
            HowToSubscribe.Example(settings);

            Console.WriteLine("Done");

            Console.ReadLine();

        }

    }


    public class MyModel
    {
        public string Email { get; set; }
    }


}
