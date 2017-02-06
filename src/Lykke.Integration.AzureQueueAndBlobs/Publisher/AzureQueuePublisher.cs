using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.Integration.AzureQueueAndBlobs.Publisher
{

    public interface IAzureQueueAndBlobSerializer<in TModel>
    {
        byte[] Serialize(TModel model);
    }

    public class AzureQueueAndBlobPublisher<TModel> : TimerPeriod, IMessageProducer<TModel>
    {
        private readonly AzureQueueAndBlobIntegrationSettings _settings;

        private IAzureQueueAndBlobSerializer<TModel> _serializer;

        public AzureQueueAndBlobPublisher(string applicationName, 
            AzureQueueAndBlobIntegrationSettings settings):base(applicationName, 1000)
        {
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();
        }

        #region Config

        public AzureQueueAndBlobPublisher<TModel> SetSerializer(IAzureQueueAndBlobSerializer<TModel> serializer)
        {
            _serializer = serializer;
            return this;
        }

        public new AzureQueueAndBlobPublisher<TModel> SetLogger(ILog log)
        {
            base.SetLogger(log);
            return this;
        }

        #endregion

        private CloudQueue _cloudQueue;
        private CloudBlobContainer _blobContainer;
        private readonly QueueWithConfirmation<TModel> _queue = new QueueWithConfirmation<TModel>();

        public override async Task Execute()
        {
            if (_cloudQueue == null)
                _cloudQueue = await _settings.GetAzureCloudQueueAsync();

            if (_blobContainer == null)
                _blobContainer = await _settings.GetAzureCloudBlobContainerAsync();

            while (true)
            {
                var message = _queue.Dequeue();

                if (message == null)
                    break;

                var dataToQueue = _serializer.Serialize(message.Item);
                var messageId = Guid.NewGuid().ToString("N");

                var blockBlob = _blobContainer.GetBlockBlobReference(messageId);

                await blockBlob.UploadFromByteArrayAsync(dataToQueue, 0, dataToQueue.Length);
                await _cloudQueue.AddMessageAsync(new CloudQueueMessage(messageId));

            }

        }


        public new AzureQueueAndBlobPublisher<TModel> Start()
        {
            base.Start();
            return this;
        }

        public Task ProduceAsync(TModel message)
        {
            _queue.Enqueue(message);
            return Task.FromResult(0);
        }

    }

}
