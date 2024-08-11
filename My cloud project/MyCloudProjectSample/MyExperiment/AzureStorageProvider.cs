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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MyExperiment
{

    /// <summary>
    /// Interface for handling storage operations related to experiments.
    /// This interface defines the necessary methods to interact with Azure Storage services 
    /// including uploading and downloading experiment-related files, processing queue messages, 
    /// and committing experiment results.
    /// </summary>
    public interface IStorageProvider
    {
        /// <summary>
        /// Commits the experiment request to the storage.
        /// </summary>
        /// <param name="request">The experiment request to commit.</param>
        Task CommitRequestAsync(IExperimentRequest request);

        /// <summary>
        /// Downloads an input file from the blob storage.
        /// </summary>
        /// <param name="fileName">The name of the file to download.</param>
        /// <returns>Returns the local path to the downloaded file.</returns>
        Task<string> DownloadInputAsync(string fileName);

        /// <summary>
        /// Receives an experiment request from the Azure Queue Storage.
        /// Processes the queue message and returns an experiment request object.
        /// </summary>
        /// <param name="token">Cancellation token to stop the operation.</param>
        /// <returns>Returns an experiment request if found, otherwise null.</returns>
        Task<IExperimentRequest> ReceiveExperimentRequestAsync(CancellationToken token);

        /// <summary>
        /// Uploads the experiment result to the blob storage and stores it in a table.
        /// </summary>
        /// <param name="experimentName">The name of the experiment.</param>
        /// <param name="result">The result object to upload.</param>
        Task UploadResultAsync(string experimentName, IExperimentResult result);
    }


    /// <summary>
    /// Implementation of the IStorageProvider interface using Azure Storage services.
    /// This class handles operations such as downloading input files from blob storage, 
    /// processing messages from an Azure Queue, uploading results to blob storage, 
    /// and saving results to Azure Table Storage.
    /// </summary>
    public class AzureStorageProvider : IStorageProvider
    {
        private readonly MyConfig _config;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly QueueClient _queueClient;
        private readonly ILogger<AzureStorageProvider> _logger;


        /// <summary>
        /// Initializes a new instance of the AzureStorageProvider class.
        /// </summary>
        /// <param name="configuration">Configuration settings from app settings.</param>
        /// <param name="logger">Logger for logging operations and errors.</param>
        public AzureStorageProvider(IConfiguration configuration, ILogger<AzureStorageProvider> logger)
        {
            _config = new MyConfig();
            configuration.GetSection("MyConfig").Bind(_config);

            var blobConnectionString = configuration.GetValue<string>("MyConfig:AzureBlobStorageConnectionString");
            var queueConnectionString = configuration.GetValue<string>("MyConfig:AzureQueueStorageConnectionString");
            var queueName = configuration.GetValue<string>("MyConfig:Queue");


            Console.WriteLine($"Blob Connection String: {blobConnectionString}"); 
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

        /// <summary>
        /// Saves the experiment result to Azure Table Storage.
        /// </summary>
        /// <param name="partitionKey">Partition key for the table entity.</param>
        /// <param name="result">Experiment result to save.</param>
        public async Task SaveResultToTableAsync(string partitionKey, IExperimentResult result)
        {
            var tableName = "experimentresults";
            var tableClient = new TableClient(_config.AzureTableStorageConnectionString, tableName);
            
            await tableClient.CreateIfNotExistsAsync();

            var entity = new ExperimentEntity
            {
                PartitionKey = partitionKey,
                RowKey = Guid.NewGuid().ToString(),  // Unique identifier for the row
                ResultJson = JsonSerializer.Serialize(result)
            };

            await tableClient.AddEntityAsync(entity);
            _logger.LogInformation($"Result saved to table with PartitionKey: {partitionKey}.");
        }

        /// <summary>
        /// Represents an entity in Azure Table Storage for storing experiment results.
        /// </summary>
        public class ExperimentEntity : ITableEntity
        {
            public string PartitionKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public string RowKey { get; set; }
            public string ResultJson { get; set; }
            public ETag ETag { get; set; }

            // Required for deserialization
            public ExperimentEntity() { }

            public ExperimentEntity(string partitionKey, string rowKey, string resultJson)
            {
                PartitionKey = partitionKey;
                RowKey = rowKey;
                ResultJson = resultJson;
            }
        }

        /// <summary>
        /// Commits the experiment request, indicating it has been processed.
        /// </summary>
        /// <param name="request">The experiment request to commit.</param>
        public async Task CommitRequestAsync(IExperimentRequest request)
        {
            _logger.LogInformation("Request committed.");
            await Task.CompletedTask; // for showing the end of method
        }

        /// <summary>
        /// Downloads an input file from Azure Blob Storage.
        /// </summary>
        /// <param name="fileName">The name of the file to download.</param>
        /// <returns>Path to the downloaded file on local storage.</returns>
        public async Task<string> DownloadInputAsync(string fileName)
        {
            try
            {
                var container = _blobServiceClient.GetBlobContainerClient("containersub4");

                _logger.LogInformation($"Attempting to download PNG file: {fileName}");
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


        private bool IsBase64String(string s)
        {
            // بررسی صحت طول رشته Base64 و ترکیب کاراکترها
            return !string.IsNullOrWhiteSpace(s) &&
                   (s.Length % 4 == 0) &&
                   Regex.IsMatch(s, @"^[a-zA-Z0-9+/=]*$");
        }
        /// <summary>
        /// Receives and processes experiment requests from the Azure Queue Storage.
        /// </summary>
        /// <param name="token">Cancellation token to allow operation cancellation.</param>
        /// <returns>Returns an experiment request if successfully processed, otherwise null.</returns>
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

                if (!IsBase64String(message.MessageText))
                {
                    _logger.LogWarning("Received message is not a valid Base64 string, skipping.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    continue;
                }

                try
                {
                    jsonMessage = Encoding.UTF8.GetString(Convert.FromBase64String(message.MessageText)); // Decode Base64

                }
                catch (FormatException ex)
                {
                    _logger.LogError(ex, "Failed to decode Base64 message.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    continue; // Continue to next message
                }

                if (!jsonMessage.Trim().EndsWith(".png", StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogWarning("Received message is not a PNG file, skipping.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    continue; // Return to the loop to get the next message
                }

                byte[] fileBytes = Convert.FromBase64String(jsonMessage);
                
                
                string outputPath = Path.Combine(Path.GetTempPath(), "output.png");
                await File.WriteAllBytesAsync(outputPath, fileBytes);

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
     
            return null;
        }

        /// <summary>
        /// Uploads the experiment result to Azure Blob Storage and saves it to Table Storage.
        /// </summary>
        /// <param name="experimentName">The name of the experiment.</param>
        /// <param name="result">The result object to upload.</param>
        public async Task UploadResultAsync(string experimentName, IExperimentResult result)
        {
            //var containerName = "outputfile";
            var containerName = "containersub4";
            var blobContainerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            await blobContainerClient.CreateIfNotExistsAsync();
            var blobName = $"{experimentName}_{DateTime.Now:yyyyMMddHHmmss}.json";
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            var json = JsonSerializer.Serialize(result);
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))

            {

                _logger.LogInformation($"Uploading result to container: {containerName}");
                _logger.LogInformation($"Blob name: {blobName}");
                await blobClient.UploadAsync(stream, overwrite: true);

            }

            _logger.LogInformation($"Uploaded result to blob: {blobClient.Uri}");

            await SaveResultToTableAsync(experimentName, result); // Save to Table Storage

        }

        public Task UploadExperimentResult(IExperimentResult result)
        {
            throw new NotImplementedException();
        }


        public async Task ProcessQueueAsync(CancellationToken token)
        {
            int emptyMessageCount = 0;
            const int maxEmptyMessages = 3; 

            while (!token.IsCancellationRequested)
            {
                var message = await ReceiveExperimentRequestAsync(token);

                if (message == null)
                {
                    emptyMessageCount++;
                    if (emptyMessageCount >= maxEmptyMessages)
                    {
                        _logger.LogInformation("No more messages in the queue. Stopping process.");
                        break; 
                    }
                    else
                    {
                        _logger.LogInformation("No messages found in the queue.");
                        await Task.Delay(5000, token);  
                        continue;
                    }
                }

                emptyMessageCount = 0; 

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
                var result = new ExperimentResult("partitionKey", "experimentId"); 
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
                
                await UploadResultAsync(result.ExperimentId, result); // Upload to Blob Storage
                await SaveResultToTableAsync(result.ExperimentId, result); // Save to Table Storage
            }
            else
            {
                _logger.LogError("Experiment result is null.");
            }

            await CommitRequestAsync(message);
        }
    }
}