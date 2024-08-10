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

            var blobConnectionString = configuration.GetValue<string>("MyConfig:AzureBlobStorageConnectionString");
            var queueConnectionString = configuration.GetValue<string>("MyConfig:AzureQueueStorageConnectionString");
            var queueName = configuration.GetValue<string>("MyConfig:Queue");


            Console.WriteLine($"Blob Connection String: {blobConnectionString}"); // To test the connection string value
            Console.WriteLine($"Queue Connection String: {queueConnectionString}");
            Console.WriteLine($"Queue Name: {queueName}");

            if (string.IsNullOrEmpty(blobConnectionString) || string.IsNullOrEmpty(queueConnectionString) || string.IsNullOrEmpty(queueName))

            {
                // If logger is not assigned yet, it might be null, ensure proper error handling
                logger?.LogError("Blob connection string or Queue connection string or Queue name is null or empty.");
                throw new ArgumentException("Blob connection string, Queue connection string, or Queue name is required.");

            }

            _blobServiceClient = new BlobServiceClient(blobConnectionString);
            _queueClient = new QueueClient(queueConnectionString, queueName);
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }


        public async Task CommitRequestAsync(IExperimentRequest request)

        {
            _logger.LogInformation("Request committed.");
            await Task.CompletedTask; // for showing the end of method
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
                throw new NotImplementedException();
            }
        }


        public async Task<IExperimentRequest> ReceiveExperimentRequestAsync(CancellationToken token)
        {
            _logger.LogInformation("Receiving experiment request from the queue.");

            while (!token.IsCancellationRequested)
            {
                QueueMessage[] messages = await _queueClient.ReceiveMessagesAsync(maxMessages: 1, visibilityTimeout: TimeSpan.FromMinutes(1), cancellationToken: token);

                if (messages.Length == 0)
                {
                    _logger.LogInformation("No messages found in the queue.");
                    await Task.Delay(1000); // One second delay
                    continue; // return to the loop to get the next message
                }

                var message = messages[0];
                string jsonMessage;


                try
                {
                    jsonMessage = Encoding.UTF8.GetString(Convert.FromBase64String(message.MessageText));


                }
                catch (FormatException ex)
                {
                    _logger.LogError(ex, "Failed to decode Base64 message.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    continue; // Continue to next message
                    
                    // return null;
                }

                _logger.LogInformation($"Received message: {jsonMessage}");

                if (!jsonMessage.Trim().StartsWith("{"))
                {
                    _logger.LogError("Received an invalid JSON message.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    continue; // Return to the loop to get the next message
                }

                try
                {
                    IExperimentRequest experimentRequest = JsonSerializer.Deserialize<ExperimentRequest>(jsonMessage);


                    if (experimentRequest == null)
                    {
                        _logger.LogError("Failed to cast ExperimentRequest to IExperimentRequest.");
                        await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                        return null;
                    }

                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    _logger.LogInformation("Message processed and deleted from the queue.");
                    return experimentRequest;
                }



                catch (JsonException ex)
                {
                    _logger.LogError(ex, $"Failed to deserialize message: {jsonMessage}");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    return null;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message from queue.");
                    throw;
                }

            }
            // Return null if no valid message was processed
            return null;

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

            //throw new NotImplementedException();

        }

        public Task UploadExperimentResult(IExperimentResult result)
        {
            throw new NotImplementedException();
        }

        public async Task ProcessQueueAsync(CancellationToken token)
        {
            int emptyMessageCount = 0;
            const int maxEmptyMessages = 3; // حداکثر تعداد پیام‌های خالی قبل از توقف پردازش

            while (!token.IsCancellationRequested)
            {
                var message = await ReceiveExperimentRequestAsync(token);

                if (message == null)
                {
                    emptyMessageCount++;
                    if (emptyMessageCount >= maxEmptyMessages)
                    {
                        _logger.LogInformation("No more messages in the queue. Stopping process.");
                        break; // توقف پردازش اگر تعداد مشخصی پیام خالی دریافت شد
                    }
                    else
                    {
                        _logger.LogInformation("No messages found in the queue.");
                        await Task.Delay(5000, token); // زمان انتظار برای بررسی دوباره صف
                        continue;
                    }
                }

                emptyMessageCount = 0; // اگر پیام جدیدی دریافت شد، شمارش خالی‌ها را ریست کنید

                // پردازش پیام
                try
                {
                    await ProcessMessageAsync(message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message.");
                }
            }
        }
        public class YourExperimentImplementation : IExperiment
        {
            public async Task<IExperimentResult> RunAsync(string inputFile)
            {
                // پیاده‌سازی منطق آزمایش
                var result = new ExperimentResult("partitionKey", "experimentId"); // اضافه کردن پارامترهای لازم

                // پردازش داده‌ها و تولید نتیجه
                return await Task.FromResult(result);

            }
        }

        private async Task ProcessMessageAsync(IExperimentRequest message)
        {
            string inputFile = await DownloadInputAsync(message.InputFile);
            if (string.IsNullOrEmpty(inputFile))
            {
                _logger.LogWarning("Input file not found, skipping message.");
                return;
            }

            IExperiment experiment = new YourExperimentImplementation();
            IExperimentResult result = await experiment.RunAsync(inputFile);
            if (result != null)
            {
                await UploadResultAsync(result.ExperimentId, result);
            }
            else
            {
                _logger.LogError("Experiment result is null.");
            }

            await CommitRequestAsync(message);


        }



        }
}