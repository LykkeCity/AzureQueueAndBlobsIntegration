using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Lykke.Integration.AzureQueueAndBlobs.Subscriber
{

    public interface IAzureQueueAndBlobDeserializer<out TModel>
    {
        TModel Deserialize(byte[] data);
    }

    public class AzureQueueAndBlobSubscriber<TModel> : TimerPeriod, IMessageConsumer<TModel>
    {
        private readonly AzureQueueAndBlobIntegrationSettings _settings;
        private readonly bool _autoDeleteFromBlob;
        private IAzureQueueAndBlobDeserializer<TModel> _deserializer;

        public AzureQueueAndBlobSubscriber(string applicationName, AzureQueueAndBlobIntegrationSettings settings, 
            bool autoDeleteFromBlob):base(applicationName, 1000)
        {
            _settings = settings;
            _autoDeleteFromBlob = autoDeleteFromBlob;
        }

        #region Configure

        public AzureQueueAndBlobSubscriber<TModel> SetDeserializer(IAzureQueueAndBlobDeserializer<TModel> deserializer)
        {
            _deserializer = deserializer;
            return this;
        }

        public new AzureQueueAndBlobSubscriber<TModel> SetLogger(ILog log)
        {
            base.SetLogger(log);
            return this;
        }

        #endregion

        private CloudQueue _cloudQueue;
        private CloudBlobContainer _blobContainer;


        public override async Task Execute()
        {
            if (_cloudQueue == null)
                _cloudQueue = await _settings.GetAzureCloudQueueAsync();

            if (_blobContainer == null)
                _blobContainer = await _settings.GetAzureCloudBlobContainerAsync();

            while (true)
            {

                var messages = (await _cloudQueue.GetMessagesAsync(28)).ToArray();

                if (messages.Length == 0)
                    break;

                foreach (var message in messages)
                {
                    var blobReference = _blobContainer.GetBlobReference(message.AsString);

                    var s = new MemoryStream();
                    await blobReference.DownloadToStreamAsync(s);

                    var model = _deserializer.Deserialize(s.ToArray());

                    foreach (var callback in _callbacks)
                        await callback(model);

                    if (_autoDeleteFromBlob)
                        await blobReference.DeleteAsync();

                    await _cloudQueue.DeleteMessageAsync(message);
                }

            }
        }



        public new AzureQueueAndBlobSubscriber<TModel> Start()
        {
            base.Start();
            return this;
        }


        private readonly List<Func<TModel, Task>> _callbacks = new List<Func<TModel, Task>>();

        void IMessageConsumer<TModel>.Subscribe(Func<TModel, Task> callback)
        {
            Subscribe(callback);
        }

        public AzureQueueAndBlobSubscriber<TModel> Subscribe(Func<TModel, Task> callback)
        {
            _callbacks.Add(callback);
            return this;
        }


    }
}
