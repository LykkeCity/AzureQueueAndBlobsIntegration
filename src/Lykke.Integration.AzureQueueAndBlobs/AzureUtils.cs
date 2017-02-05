using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.Integration.AzureQueueAndBlobs
{

    public static class AzureUtils
    {
        public static async Task<CloudQueue> GetAzureCloudQueueAsync(this AzureQueueAndBlobIntegrationSettings settings)
        {
            var storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(settings.QueueName);
            await queue.CreateIfNotExistsAsync();
            return queue;
        }

        public static async Task<CloudBlobContainer> GetAzureCloudBlobContainerAsync(this AzureQueueAndBlobIntegrationSettings settings)
        {

            var storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();

            var containerRef = blobClient.GetContainerReference(settings.BlobContainer);

            if (!await containerRef.ExistsAsync())
                await containerRef.CreateAsync();

            return containerRef;
        }

    }

}
