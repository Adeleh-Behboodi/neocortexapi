using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MyCloudProject.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MyExperiment
{


    public interface IStorageProvider
    {
        Task CommitRequestAsync(IExperimentRequest request);
        Task<string> DownloadInputAsync(string fileName);
        Task<IExperimentRequest> ReceiveExperimentRequestAsync(CancellationToken token);
        Task UploadResultAsync(string experimentName, IExperimentResult result);

    }



    public class AzureStorageProvider : IStorageProvider
    {
        private readonly MyConfig _config;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly QueueClient _queueClient;
        private readonly ILogger<AzureStorageProvider> _logger;


        public AzureStorageProvider(IConfiguration configuration, ILogger<AzureStorageProvider> logger)
        {
            _config = new MyConfig();
            configuration.GetSection("MyConfig").Bind(_config);

            var blobConnectionString = configuration.GetValue<string>("AzureBlobStorageConnectionString");

            Console.WriteLine($"Blob Connection String: {blobConnectionString}"); // To test the connection string value
            var queueConnectionString = configuration.GetValue<string>("AzureQueueStorageConnectionString");
            var queueName = configuration.GetValue<string>("AzureQueueName");
            Console.WriteLine($"Queue Connection String: {queueConnectionString}");
            Console.WriteLine($"Queue Name: {queueName}");



            if (string.IsNullOrEmpty(blobConnectionString) || string.IsNullOrEmpty(queueConnectionString) || string.IsNullOrEmpty(queueName))
            {
                // If logger is not assigned yet, it might be null, ensure proper error handling
                logger?.LogError("Blob connection string is null or empty.");
                throw new ArgumentException("Blob connection string is required.");
            }

            _blobServiceClient = new BlobServiceClient(blobConnectionString);
            _queueClient = new QueueClient(queueConnectionString, queueName);
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        }


        public Task CommitRequestAsync(IExperimentRequest request)
        {
            throw new NotImplementedException();
        }


        public async Task<string> DownloadInputAsync(string fileName)
        {
            try
            {
                var container = _blobServiceClient.GetBlobContainerClient("containersub4");
                await container.CreateIfNotExistsAsync();

                var blob = container.GetBlobClient(fileName);

                if (await blob.ExistsAsync())
                {
                    var downloadResponse = await blob.DownloadAsync();
                    var localFilePath = Path.Combine(Path.GetTempPath(), fileName);

                    using (var fileStream = File.OpenWrite(localFilePath))
                    {
                        await downloadResponse.Value.Content.CopyToAsync(fileStream);
                    }

                    _logger.LogInformation($"File downloaded to: {localFilePath}");
                    return localFilePath;
                }
                else
                {
                    _logger.LogWarning($"Blob {fileName} does not exist in container.");
                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while downloading blob.");
                throw;

            }
        }


        public async Task<IExperimentRequest> ReceiveExperimentRequestAsync(CancellationToken token)

        {
            _logger.LogInformation("Receiving experiment request from the queue.");

            QueueMessage[] messages = await _queueClient.ReceiveMessagesAsync(maxMessages: 1, visibilityTimeout: TimeSpan.FromMinutes(1), cancellationToken: token);

            if (messages.Length == 0)
            {
                _logger.LogInformation("No messages found in the queue.");
                return null;
            }

            var message = messages[0];
            var messageText = message.MessageText;

            _logger.LogInformation($"Received message: {messageText}");

            try
            {
                var experimentRequest = JsonSerializer.Deserialize<IExperimentRequest>(messageText);


                await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);

                _logger.LogInformation("Message processed and deleted from the queue.");
                return experimentRequest;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message from queue.");
                throw;


            }
        }




        public async Task UploadResultAsync(string experimentName, IExperimentResult result)
        {
            //var containerName = "outputfile";
            var containerName = "containersub4";

            _logger.LogInformation($"Uploading result to container: {containerName}");

            var blobContainerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            var blobName = $"{experimentName}_{DateTime.Now:yyyyMMddHHmmss}.json";
            _logger.LogInformation($"Blob name: {blobName}");

            var blobClient = blobContainerClient.GetBlobClient(blobName);

            var json = JsonSerializer.Serialize(result);
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                await blobClient.UploadAsync(stream, overwrite: true);
            }

            _logger.LogInformation($"Uploaded result to blob: {blobClient.Uri}");



        }

        public Task UploadExperimentResult(IExperimentResult result)
        {
            throw new NotImplementedException();
        }


    }


}
