using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Common;
using Common.Log;

namespace Lykke.Integration.AzureQueueAndBlobs.Subscriber
{

    public interface IAzureQueueMessageDeserializer<out TModel>
    {
        TModel Deserialize(byte[] data);
    }

    public class AzureQueueSubscriber<TModel> : IStartable, IMessageConsumer<TModel>
    {
        private readonly string _applicationName;
        private readonly AzureQueueAndBlobIntegrationSettings _settings;
        private readonly bool _autoDeleteFromBlob;
        private IAzureQueueMessageDeserializer<TModel> _deserializer;
        private ILog _log;

        public AzureQueueSubscriber(string applicationName, AzureQueueAndBlobIntegrationSettings settings, 
            bool autoDeleteFromBlob)
        {
            _applicationName = applicationName;
            _settings = settings;
            _autoDeleteFromBlob = autoDeleteFromBlob;
        }


        #region Configure

        public AzureQueueSubscriber<TModel> SetDeserializer(IAzureQueueMessageDeserializer<TModel> deserializer)
        {
            _deserializer = deserializer;
            return this;
        }

        public AzureQueueSubscriber<TModel> SetLogger(ILog log)
        {
            _log = log;
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
                    var blob = await _settings.GetAzureCloudBlobContainerAsync();

                    while (_task != null)
                    {

                        var messages = (await queue.GetMessagesAsync(31)).ToArray();

                        if (messages.Length == 0)
                        {
                            await Task.Delay(1000);
                            continue;
                        }

                        foreach (var message in messages)
                        {
                            var blobReference = blob.GetBlobReference(message.AsString);

                            var s = new MemoryStream();
                            await blobReference.DownloadToStreamAsync(s);

                            var model = _deserializer.Deserialize(s.ToArray());

                            foreach (var callback in _callbacks)
                                await callback(model);

                            if (_autoDeleteFromBlob)
                                await blobReference.DeleteAsync();

                            await queue.DeleteMessageAsync(message);
                        }

                    }

                }
                catch (Exception e)
                {
                    await _log.WriteErrorAsync(_applicationName, "TheTask", "", e);
                }

        }


        public AzureQueueSubscriber<TModel> Start()
        {
            if (_task != null)
                return this;


            if (_deserializer == null)
                throw new Exception("Deserializer required for: "+_applicationName);

            if (_log == null)
                throw new Exception("ILog required for: " + _applicationName);

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

        private readonly List<Func<TModel, Task>> _callbacks = new List<Func<TModel, Task>>();

        public void Subscribe(Func<TModel, Task> callback)
        {
            _callbacks.Add(callback);
        }

    }
}
