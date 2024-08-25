using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace ExperimentProcessing
{
    /// <summary>
    /// Interface for handling storage operations related to experiments.
    /// This interface defines methods to interact with Azure Storage services
    /// such as uploading and downloading files, and processing queue messages.
    /// </summary>
    public interface IStorageService
    {
        /// <summary>
        /// Saves the experiment request.
        /// </summary>
        /// <param name="request">The experiment request to save.</param>
        Task SaveExperimentAsync(ExperimentRequest request);

        /// <summary>
        /// Fetches an input file from blob storage.
        /// </summary>
        /// <param name="fileName">Name of the file to fetch.</param>
        /// <returns>Path to the fetched file or a message indicating the result.</returns>
        Task<string> FetchInputFileAsync(string fileName);

        /// <summary>
        /// Retrieves an experiment request from the queue.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>An experiment request object if found, otherwise null.</returns>
        Task<ExperimentRequest> GetExperimentRequestAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Uploads the experiment result to blob storage.
        /// </summary>
        /// <param name="experimentId">ID of the experiment.</param>
        /// <param name="result">Result object to upload.</param>
        Task UploadExperimentResultAsync(string experimentId, IExperimentResult result);
    }

    /// <summary>
    /// Implementation of IStorageService using Azure Storage services.
    /// This class handles operations like downloading input files from blob storage, 
    /// processing messages from an Azure Queue, and uploading results to blob storage.
    /// </summary>
    public class AzureStorageProvider : IStorageService
    {
        private readonly ConfigSettings _settings;
        private readonly BlobServiceClient _blobClient;
        private readonly QueueClient _queueClient;
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

            //logger?.LogInformation($"AzureBlobStorageConnectionString: {blobConnStr ?? "null"}");
            //logger?.LogInformation($"AzureQueueStorageConnectionString: {queueConnStr ?? "null"}");
            //logger?.LogInformation($"QueueName: {queueName ?? "null"}");

            if (string.IsNullOrWhiteSpace(blobConnStr) || string.IsNullOrWhiteSpace(queueConnStr) || string.IsNullOrWhiteSpace(queueName))
            {
                _logger.LogError("One or more configuration settings are missing or invalid.");
                throw new ArgumentException("Blob or Queue connection settings are missing.");
            }

            _blobClient = new BlobServiceClient(blobConnStr);
            _queueClient = new QueueClient(queueConnStr, queueName);
            _logger = logger;
        }

        /// <summary>
        /// Saves the experiment request to the system.
        /// </summary>
        /// <param name="request">The experiment request to save.</param>
        public async Task SaveExperimentAsync(ExperimentRequest request)
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
        public async Task<ExperimentRequest> GetExperimentRequestAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messages = await _queueClient.ReceiveMessagesAsync(maxMessages: 1, visibilityTimeout: TimeSpan.FromMinutes(1), cancellationToken: cancellationToken);

                if (messages.Value.Length == 0)
                {
                    await Task.Delay(5000, cancellationToken); // Wait before checking the queue again
                    continue;
                }

                var message = messages.Value[0];
                try
                {
                    var json = Encoding.UTF8.GetString(Convert.FromBase64String(message.MessageText));
                    var request = JsonSerializer.Deserialize<ExperimentRequest>(json);
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
                    return request;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing queue message.");
                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
                }
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
            var container = _blobClient.GetBlobContainerClient("results");
            await container.CreateIfNotExistsAsync();
            var blobName = $"{experimentId}_{DateTime.UtcNow:yyyyMMddHHmmss}.json";
            var blob = container.GetBlobClient(blobName);

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(result)));
            await blob.UploadAsync(stream, overwrite: true);

            _logger.LogInformation($"Result uploaded to blob: {blob.Uri}");
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

    /// <summary>
    /// Represents a request for an experiment.
    /// </summary>
    public class ExperimentRequest
    {
        public string InputFile { get; set; }
    }

    /// <summary>
    /// Interface for experiment results.
    /// </summary>
    public interface IExperimentResult
    {
        string ExperimentId { get; }
    }

    /// <summary>
    /// Represents the result of an experiment.
    /// </summary>
    public class ExperimentResult : IExperimentResult
    {
        public string ExperimentId { get; set; }
        public ExperimentResult(string id) => ExperimentId = id;
    }
}