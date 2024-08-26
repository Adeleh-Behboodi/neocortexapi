using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MyCloudProject.Common;
using MyExperiment;
using System;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace ExperimentProcessing
{
    /// <summary>
    /// Implementation of IStorageService using Azure Storage services.
    /// This class handles operations like downloading input files from blob storage, 
    /// processing messages from an Azure Queue, and uploading results to blob storage.
    /// </summary>
    public class AzureStorageProvider : IStorageProvider
    {
        private readonly ConfigSettings _settings;
        private readonly BlobServiceClient _blobClient;
        private readonly QueueClient _queueClient;
        private readonly TableClient _tableClient;
        private readonly ILogger<AzureStorageProvider> _logger;

        /// <summary>
        /// Initializes a new instance of StorageService.
        /// </summary>
        /// <param name="configuration">Configuration settings from app settings.</param>
        /// <param name="logger">Logger for logging operations and errors.</param>
        public AzureStorageProvider(IConfiguration configurationRoot, ILogger<AzureStorageProvider> logger)
        {
            _settings = configurationRoot.GetSection("ConfigSettings").Get<ConfigSettings>();
            var blobConnStr = configurationRoot.GetValue<string>("AzureBlobStorageConnectionString");
            var queueConnStr = configurationRoot.GetValue<string>("AzureQueueStorageConnectionString");
            var queueName = configurationRoot.GetValue<string>("Queue");
            var tableName = configurationRoot.GetValue<string>("ResultTable");

            logger?.LogInformation($"AzureBlobStorageConnectionString: {blobConnStr ?? "null"}");
            logger?.LogInformation($"AzureQueueStorageConnectionString: {queueConnStr ?? "null"}");
            logger?.LogInformation($"QueueName: {queueName ?? "null"}");
            logger?.LogInformation($"TableName: {tableName ?? "null"}");


            if (string.IsNullOrWhiteSpace(blobConnStr) || string.IsNullOrWhiteSpace(queueConnStr) || string.IsNullOrWhiteSpace(queueName))
            {
                _logger.LogError("One or more configuration settings are missing or invalid.");
                throw new ArgumentException("Blob or Queue connection settings are missing.");
            }

            _blobClient = new BlobServiceClient(blobConnStr);
            _queueClient = new QueueClient(queueConnStr, queueName);
            _tableClient = new TableClient(blobConnStr, tableName);

            _logger = logger;
        }

        /// <summary>
        /// Saves the experiment request to the system.
        /// </summary>
        /// <param name="request">The experiment request to save.</param>
        public async Task SaveExperimentAsync(ExerimentRequest request)
        {
            try
            {
                if (request == null)
                {
                    throw new ArgumentNullException(nameof(request), "Request cannot be null.");
                }
                _logger.LogInformation("Experiment request saved.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving experiment request.");
                throw;
            }
        }

        /// <summary>
        /// Fetches an input file from blob storage.
        /// </summary>
        /// <param name="fileName">The name of the file to fetch.</param>
        /// <returns>Path to the fetched file or a message indicating the result.</returns>
        public async Task<string> FetchInputFileAsync(string fileName)
        {
            var container = _blobClient.GetBlobContainerClient("containersub4");
            var blob = container.GetBlobClient("8.png");

            if (!await container.ExistsAsync() || !await blob.ExistsAsync())
            {
                _logger.LogWarning($"Blob or container not found.");
                return null;
            }

            using var memoryStream = new MemoryStream();
            await blob.DownloadToAsync(memoryStream);
            memoryStream.Position = 0;

            return "File fetched and processed.";
        }

        /// <summary>
        /// Retrieves an experiment request from the Azure Queue.
        /// Processes the message and returns the experiment request.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>An experiment request object if found, otherwise null.</returns>
        public async Task<ExerimentRequest> GetExperimentRequestAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messages = await _queueClient.ReceiveMessagesAsync
                    (maxMessages: 1, 
                    visibilityTimeout: TimeSpan.FromMinutes(1), 
                    cancellationToken: cancellationToken);
                _logger.LogInformation($"Received {messages.Value.Length} message(s) from the queue.");

                if (messages.Value.Length == 0)
                {
                    await Task.Delay(5000, cancellationToken);
                    _logger.LogInformation("Queue is empty. No messages found.");
                    return null; 
                    //continue;
                }

                var message = messages.Value[0];
                try
                {
                    var json = Encoding.UTF8.GetString(Convert.FromBase64String(message.MessageText));
                    //var json = message.MessageText;
                    _logger.LogInformation($"Message content: {json}"); 
                    Console.WriteLine($"JSON: {json}");
                    
                    var request = JsonSerializer.Deserialize<ExerimentRequest>(json);
                   // await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
                    return request;
                }
                catch (JsonException ex)
                {
                    _logger.LogError($"........................JSON deserialization error: {ex.Message}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing queue message.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
                }
                //finally
                //{
                //    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
                //}
            }

            return null;
        }

        /// <summary>
        /// Uploads the experiment result to Azure Blob Storage.
        /// </summary>
        /// <param name="experimentId">ID of the experiment.</param>
        /// <param name="result">Result object to upload.</param>
        public async Task UploadExperimentResultAsync(string experimentId, IExperimentResult result)
        {
            var container = _blobClient.GetBlobContainerClient("outputfile");

            await container.CreateIfNotExistsAsync();
            var blobName = $"{experimentId}_{DateTime.UtcNow:yyyyMMddHHmmss}.json";
            var blob = container.GetBlobClient(blobName);

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(result)));
            await blob.UploadAsync(stream, overwrite: true);

            _logger.LogInformation($"Result uploaded to blob: {blob.Uri}");
        }

        public async Task ProcessAndUploadResultsAsync()
        {
            var container = _blobClient.GetBlobContainerClient("outputfile");

            if (!await container.ExistsAsync())
            {
                _logger.LogWarning("Blob container 'outputfile' does not exist.");
                return;
            }

            await foreach (var blobItem in container.GetBlobsAsync())
            {
                var blobClient = container.GetBlobClient(blobItem.Name);
                var stream = new MemoryStream();

                try
                {
                    await blobClient.DownloadToAsync(stream);
                    stream.Position = 0; 

                    var json = Encoding.UTF8.GetString(stream.ToArray());
                    var result = JsonSerializer.Deserialize<IExperimentResult>(json);

                    if (result != null)
                    {
                        await UploadExperimentResultToTableAsync(result);
                    }
                    else
                    {
                        _logger.LogWarning($"Failed to deserialize the blob content for blob: {blobItem.Name}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error processing blob: {blobItem.Name}");
                }
            }
        }

        public async Task UploadExperimentResultToTableAsync(IExperimentResult result)
        {
            try
            {
                if (result == null)
                {
                    throw new ArgumentNullException(nameof(result), "Experiment result cannot be null.");
                }

                var tableName = "tablesub4";
                var tableClient = _tableClient;

                await _tableClient.CreateIfNotExistsAsync();

                var entity = new ExperimentResultEntity
                {
                    PartitionKey = result.ExperimentId,
                    RowKey = $"{result.ExperimentId}_{DateTime.UtcNow:yyyyMMddHHmmss}",
                    ExperimentId = result.ExperimentId,
                    StartTimeUtc = result.StartTimeUtc,
                    EndTimeUtc = result.EndTimeUtc,
                    Duration = result.Duration,
                    InputFileUrl = result.InputFileUrl
                };

                await tableClient.UpsertEntityAsync(entity, TableUpdateMode.Merge);

                _logger.LogInformation($"Experiment result uploaded to table: {tableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error uploading experiment result to table.");
                throw;
            }
        }

    }




    /// <summary>
    /// Configuration settings for the AzureStorageProvider.
    /// </summary>
    public class ConfigSettings
    {
        public string ResultTableName { get; set; }
        public string TableStorageConnectionString { get; set; }
    }

}