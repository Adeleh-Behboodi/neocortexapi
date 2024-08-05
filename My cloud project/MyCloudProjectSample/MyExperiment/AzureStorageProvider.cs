using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
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
    public class AzureStorageProvider : IStorageProvider
    {
        private MyConfig _config;

        public AzureStorageProvider(IConfigurationSection configSection)
        {
            _config = new MyConfig();
            configSection.Bind(_config);
        }

        public Task CommitRequestAsync(IExerimentRequest request)
        {
            throw new NotImplementedException();
        }

        public async Task<string> DownloadInputAsync(string fileName)
        {
            BlobContainerClient container = new BlobContainerClient("Read from config","sample-file");

            await container.CreateIfNotExistsAsync();

            BlobClient blob = container.GetBlobClient(fileName);
            
            
            throw new NotImplementedException();
        }






        public IExerimentRequest ReceiveExperimentRequestAsync(CancellationToken token)
        {
            // Receive the message and make sure that it is serialized to IExperimentResult.
            throw new NotImplementedException();
        }


        public Task UploadExperimentResult(IExperimentResult result)
        {
            throw new NotImplementedException();
        }


        private readonly BlobServiceClient _blobServiceClient;
        private readonly ILogger<AzureStorageProvider> _logger;


        public AzureStorageProvider(IConfiguration configuration, ILogger<AzureStorageProvider> logger)
        
        {
            // Initial setting BlobServiceClient
            var connectionString = configuration.GetValue<string>("AzureBlobStorageConnectionString");
            _blobServiceClient = new BlobServiceClient(connectionString);
            _logger = logger;
        }


        public async Task UploadResultAsync(string experimentName, IExperimentResult result)
        {

            // Name of container
            var containerName = "outputfile";

            _logger.LogInformation($"Uploading result to container: {containerName}");

            // Create a BlobContainerClient for the specified container
            var blobContainerClient = _blobServiceClient.GetBlobContainerClient(containerName);

            // Create the container if it does not already exist
            await blobContainerClient.CreateIfNotExistsAsync();

            // Generate the output file name using the experiment name and current time
            var blobName = $"{experimentName}_{DateTime.Now:yyyyMMddHHmmss}.json";
            _logger.LogInformation($"Blob name: {blobName}");


            var blobClient = blobContainerClient.GetBlobClient(blobName);

            // Serialize the result to JSON
            var json = JsonSerializer.Serialize(result);
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                // Upload the file to Blob Storage
                await blobClient.UploadAsync(stream, overwrite: true);
            }

            _logger.LogInformation($"Uploaded result to blob: {blobClient.Uri}");


        }
    }


}
