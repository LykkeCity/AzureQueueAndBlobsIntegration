using System;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Integration.AzureQueueAndBlobs;
using Lykke.Integration.AzureQueueAndBlobs.Subscriber;

namespace TestInvoke.Subscriber
{
    public class TestDeserializer : IAzureQueueAndBlobDeserializer<MyModel>
    {
        public MyModel Deserialize(byte[] data)
        {
            var json = Encoding.UTF8.GetString(data);
            return json.DeserializeJson<MyModel>();
        }
    }

    public static class HowToSubscribe
    {

        private static AzureQueueAndBlobSubscriber<MyModel> _publisher;
        public static void Example(AzureQueueAndBlobIntegrationSettings settings)
        {
            var log = new LogToConsole();

            _publisher
                = new AzureQueueAndBlobSubscriber<MyModel>("myname", settings, true)
                .SetLogger(log)
                .SetDeserializer(new TestDeserializer())
                .Subscribe(Handle)
                .Start();
        }

        public static void Stop()
        {
            _publisher.Stop();
        }


        public static Task Handle(MyModel itm)
        {
            Console.WriteLine(itm.Email);
            return Task.FromResult(0);
        }
    }
}
