using System;
using System.Threading.Tasks;
using Autofac;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.Integration.AzureQueueAndBlobs.Publisher
{

    public interface IAzureQueueSerializer<in TModel>
    {
        byte[] Serialize(TModel model);
    }

    public class AzureQueuePublisher<TModel> : IStartable, IMessageProducer<TModel>
    {
        private readonly string _applicationName;
        private readonly AzureQueueAndBlobIntegrationSettings _settings;

        private IAzureQueueSerializer<TModel> _serializer;
        private ILog _log;

        public AzureQueuePublisher(string applicationName, AzureQueueAndBlobIntegrationSettings settings)
        {
            _applicationName = applicationName;
            _settings = settings;
            _settings.QueueName = _settings.QueueName.ToLower();
        }

        #region Config

        public AzureQueuePublisher<TModel> SetLogger(ILog log)
        {
            _log = log;
            return this;
        }

        public AzureQueuePublisher<TModel> SetSerializer(IAzureQueueSerializer<TModel> serializer)
        {
            _serializer = serializer;
            return this;
        }


        #endregion


        private Task _task;

        private async Task TheTask()
        {

            while (_task != null)
                try
                {

                    var queue = await _settings.GetAzureCloudQueueAsync();
                    var blobContainer = await _settings.GetAzureCloudBlobContainerAsync();

                    while (true)
                    {
                        var message = _queue.Dequeue();
                        if (message == null)
                        {
                            if (_task == null)
                                break;

                            await Task.Delay(1000);
                            continue;
                        }

                        var dataToQueue = _serializer.Serialize(message.Item);
                        var messageId = Guid.NewGuid().ToString("N");

                        var blockBlob = blobContainer.GetBlockBlobReference(messageId);

                        await blockBlob.UploadFromByteArrayAsync(dataToQueue,0, dataToQueue.Length);
                        await queue.AddMessageAsync(new CloudQueueMessage(blockBlob.Uri.AbsoluteUri));

                        message.Compliete();
                    }

                }
                catch (Exception e)
                {
                    await _log.WriteErrorAsync(_applicationName, "TheTask", "", e);
                }
        }


        public AzureQueuePublisher<TModel> Start()
        {
            if (_task != null)
                return this;

            if (_log == null)
                throw new Exception("ILog required for: "+_applicationName);

            if (_serializer == null)
                throw new Exception("IAzureQueueSerializer required for: " + _applicationName);

            _task = TheTask();

            return this;
        }

        void IStartable.Start()
        {
            Start();
        }


        public void Stop()
        {
            if (_task == null)
                return;

            var task = _task;
            _task = null;
            task.Wait();
        }

        private readonly QueueWithConfirmation<TModel> _queue = new QueueWithConfirmation<TModel>();

        public Task ProduceAsync(TModel message)
        {
            _queue.Enqueue(message);
            return Task.FromResult(0);
        }
    }
}
