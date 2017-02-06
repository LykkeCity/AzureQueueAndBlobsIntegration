using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Integration.AzureQueueAndBlobs;
using Lykke.Integration.AzureQueueAndBlobs.Publisher;

namespace TestInvoke.Publisher
{
    public class TestSerializer : IAzureQueueAndBlobSerializer<MyModel>
    {
        public byte[] Serialize(MyModel model)
        {
            return model.ToJson().ToUtf8Bytes();
        }
    }

    public static class HowToPublish
    {

        public static async Task Example(AzureQueueAndBlobIntegrationSettings settings)
        {
            var log = new LogToConsole();

            var publisher
                = new AzureQueueAndBlobPublisher<MyModel>("myname", settings)
                .SetLogger(log)
                .SetSerializer(new TestSerializer())
                .Start();

            await publisher.ProduceAsync(new MyModel {Email = "my@email.com"});
        }

    }
}
