using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MyCloudProject.Common;
using MyExperiment.MyExperiment;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        Task CommitRequestAsync(ExerimentRequest request);

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
        Task<ExerimentRequest> ReceiveExperimentRequestAsync(CancellationToken token);

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
        private readonly IConfiguration _configuration;



        /// <summary>
        /// Initializes a new instance of the AzureStorageProvider class.
        /// </summary>
        /// <param name="configuration">Configuration settings from app settings.</param>
        /// <param name="logger">Logger for logging operations and errors.</param>
        public AzureStorageProvider(IConfiguration configuration, ILogger<AzureStorageProvider> logger)
        {
            _configuration = configuration;
            configuration.GetSection("MyConfig").Bind(_config);

            var blobConnectionString = configuration.GetValue<string>("AzureBlobStorageConnectionString").ToString();
            var queueConnectionString = configuration.GetValue<string>("AzureQueueStorageConnectionString").ToString();
            var queueName = configuration.GetValue<string>("Queue").ToString();

            if (string.IsNullOrEmpty(blobConnectionString) || string.IsNullOrEmpty(queueConnectionString) || string.IsNullOrEmpty(queueName))

            {
                logger?.LogError("Blob connection string or Queue connection string or Queue name is null or empty.");
                throw new ArgumentException("Blob connection string, Queue connection string, or Queue name is required.");
            }

            _blobServiceClient = new BlobServiceClient(blobConnectionString);
            _queueClient = new QueueClient(queueConnectionString, queueName);
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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
        /// Saves the experiment result to Azure Table Storage.
        /// </summary>
        /// <param name="partitionKey">Partition key for the table entity.</param>
        /// <param name="result">Experiment result to save.</param>
        public async Task SaveResultToTableAsync(string partitionKey, IExperimentResult result)
        {
            var tableName = _config.ResultTable;
            var tableClient = new TableClient(_config.AzureTableStorageConnectionString, tableName);

            await tableClient.CreateIfNotExistsAsync();

            var entity = new ExperimentEntity
            {
                PartitionKey = partitionKey,
                RowKey = Guid.NewGuid().ToString(),
                ResultJson = JsonSerializer.Serialize(result)
            };

            await tableClient.AddEntityAsync(entity);
            _logger.LogInformation($"Result saved to table with PartitionKey: {partitionKey}.");
        }



        /// <summary>
        /// Commits the experiment request, indicating it has been processed.
        /// </summary>
        /// <param name="request">The experiment request to commit.</param>
        public async Task CommitRequestAsync(ExerimentRequest request)
        {
            _logger.LogInformation("Request committed.");
            await Task.CompletedTask; 
        }



        /// <summary>
        /// Downloads an input file from Azure Blob Storage.
        /// </summary>
        /// <param name="fileName">The name of the file to download.</param>
        /// <returns>Path to the downloaded file on local storage.</returns>
        /// 
        public async Task<string> DownloadInputAsync(string fileName)
        {
            return await DownloadInputAsync(fileName, isOutput: false);
        }
        public async Task<string> DownloadInputAsync(string fileName, bool isOutput = false)
        {
            var containerName = isOutput ? "outputfile" : "containersub4";


            var container = _blobServiceClient.GetBlobContainerClient(containerName);
            var blobClient = container.GetBlobClient(fileName);

            _logger.LogInformation($"Attempting to download file: {fileName} from container: {containerName}");

            try
            {
                if (await container.ExistsAsync())
                {
                    _logger.LogInformation($"Container exists: {containerName}");



                    if (await blobClient.ExistsAsync())
                    {
                        var downloadResponse = await blobClient.DownloadAsync();

                        using (var memoryStream = new MemoryStream())
                        {
                            await downloadResponse.Value.Content.CopyToAsync(memoryStream);
                            memoryStream.Position = 0;

                            if (isOutput)
                            {
                                var convertedStream = await ConvertFileToOutputFormatAsync(memoryStream);
                                await UploadFileToOutputContainerAsync(fileName, convertedStream);
                                return blobClient.Uri.ToString();

                            }
                            else
                            {

                                if (containerName == "containersub4" && !IsPngFile(memoryStream))
                                {
                                    _logger.LogWarning($"File downloaded is not a valid PNG.");
                                    return null;
                                }
                                else
                                {
                                    _logger.LogInformation($"File downloaded successfully.");
                                    return "File downloaded to memory";
                                }
                            }
                        }
                    }

                    else
                    {
                        _logger.LogWarning($"Blob {fileName} does not exist in container.");
                        return null;
                    }
                }
                else
                {
                    _logger.LogWarning($"Container {containerName} does not exist.");
                    return null;
                }
            }

            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while downloading or validating blob.");
                throw;
            }
        }




        private async Task UploadFileToOutputContainerAsync(string fileName, Stream fileStream)
        {
            var containerName = "outputfile";
            var container = _blobServiceClient.GetBlobContainerClient(containerName);
            var blobClient = container.GetBlobClient(fileName);

            _logger.LogInformation($"Attempting to upload file: {fileName} to container: {containerName}");

            fileStream.Position = 0; // Ensure the stream position is at the beginning
            await blobClient.UploadAsync(fileStream, overwrite: true);

            _logger.LogInformation($"File uploaded to: {blobClient.Uri}");
        }

        private async Task<Stream> ConvertFileToOutputFormatAsync(Stream inputStream)
        {
            var outputStream = new MemoryStream();
            // Conversion logic here
            return outputStream;
        }


        private bool IsBase64String(string s)
        {
            return !string.IsNullOrWhiteSpace(s) &&
                   (s.Length % 4 == 0) &&
                   Regex.IsMatch(s, @"^[a-zA-Z0-9+/=]*$");
        }


        /// <summary>
        /// Checks if the stream represents a PNG file.
        /// </summary>
        private bool IsPngFile(Stream fileStream)
        {
            try
            {
                byte[] header = new byte[8];
                fileStream.Read(header, 0, header.Length);
                fileStream.Position = 0; // Reset stream position after reading

                byte[] pngSignature = { 137, 80, 78, 71, 13, 10, 26, 10 };

                return header.SequenceEqual(pngSignature);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while validating PNG file.");
                return false;
            }
        }






        /// <summary>
        /// Receives and processes experiment requests from the Azure Queue Storage.
        /// </summary>
        /// <param name="token">Cancellation token to allow operation cancellation.</param>
        /// <returns>Returns an experiment request if successfully processed, otherwise null.</returns>
        public async Task<ExerimentRequest> ReceiveExperimentRequestAsync(CancellationToken token)
        {
            _logger.LogInformation("Receiving experiment request from the queue.");

            while (!token.IsCancellationRequested)
            {
                try
                {
                    QueueMessage[] messages = await _queueClient.ReceiveMessagesAsync(maxMessages: 1, visibilityTimeout: TimeSpan.FromMinutes(1), cancellationToken: token);

                    if (messages.Length == 0)
                    {
                        _logger.LogInformation("No messages found in the queue. Waiting for new messages...");
                        await Task.Delay(5000, token); 
                        continue;
                    }

                    var message = messages[0];
                    _logger.LogInformation($"Received message: {message.MessageText}");

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
                        _logger.LogInformation($"Decoded message: {jsonMessage}");
                    }
                    catch (FormatException ex)
                    {
                        _logger.LogError(ex, "Failed to decode Base64 message.");
                        await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                        continue;
                    }

                    if (!jsonMessage.Trim().StartsWith("{"))
                    {
                        _logger.LogWarning("Received message is not a valid JSON, skipping.");
                        await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                        continue;
                    }

                    var experimentRequest = JsonSerializer.Deserialize<ExerimentRequest>(jsonMessage);

                    if (experimentRequest == null)
                    {
                        _logger.LogWarning("Deserialization returned null.");
                        await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                        continue;
                    }

                    _logger.LogInformation($"Deserialized experiment request: {JsonSerializer.Serialize(experimentRequest)}");

                    await _queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, token);
                    _logger.LogInformation("Message processed and deleted from the queue.");
                    return experimentRequest;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred while processing message from the queue.");
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
            var containerName = "outputfile"; 
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

        }



        /// <summary>
        /// 
        /// </summary>
        public async Task ProcessQueueAsync(CancellationToken token)
        {

            int fileCount = 0;
            const int maxFilesPerRun = 5;

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

                if (fileCount >= maxFilesPerRun)
                {
                    _logger.LogInformation("Reached maximum number of files to process in this run.");
                    break;
                }
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



        /// <summary>
        /// 
        /// </summary>
        public class YourExperimentImplementation : IExperiment
        {
            public async Task<IExperimentResult> RunAsync(string inputFile)
            {
                var result = new ExperimentResult("partitionKey", "experimentId");
                return await Task.FromResult(result);

            }
        }




        /// <summary>
        /// 
        /// </summary>
        private async Task ProcessMessageAsync(ExerimentRequest message)
        {
            _logger.LogInformation($"Processing message with InputFile: {message.InputFile}");

            string inputFile = await DownloadInputAsync(message.InputFile, isOutput: false);


            if (string.IsNullOrEmpty(inputFile))
            {
                _logger.LogWarning("Input file not found or invalid, skipping message.");
                return;
            }

            _logger.LogInformation($"Input file downloaded: {inputFile}");

            IExperiment experiment = new YourExperimentImplementation(); 
            IExperimentResult result = await experiment.RunAsync(inputFile);

            if (result != null)
            {
                await UploadResultAsync(result.ExperimentId, result);

                var resultJson = JsonSerializer.Serialize(result);
                await SendMessageToQueueAsync(resultJson);

                await SaveResultToTableAsync(result.ExperimentId, result);
            }
            else
            {
                _logger.LogError("Experiment result is null.");
            }
        }



        /// <summary>
        /// 
        /// </summary>
        private async Task SendMessageToQueueAsync(string messageText)
        {
            var triggerQueueName = "trigger-queue"; 
            var triggerQueueClient = new QueueClient(_configuration.GetValue<string>("AzureQueueStorageConnectionString"), triggerQueueName);
            await triggerQueueClient.CreateIfNotExistsAsync();

            if (await triggerQueueClient.ExistsAsync())
            {
                await triggerQueueClient.SendMessageAsync(Convert.ToBase64String(Encoding.UTF8.GetBytes(messageText)));
                _logger.LogInformation("Message sent to trigger queue.");
            }
            else
            {
                _logger.LogWarning($"Queue {triggerQueueName} does not exist.");
            }
        }
    }
}

