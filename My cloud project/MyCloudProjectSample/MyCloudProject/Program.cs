using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues.Models;
using Azure.Storage.Queues;
using System.Text.Json;
using System.Text;
using MyExperiment;
using ExperimentProcessing;
using MyCloudProject.Common;


namespace MyCloudProject
{
    class Program
    {
        /// <summary>
        /// Identifies the project.
        /// </summary>
        private static string _projectName = "ML 23/24-4";

        static async Task Main(string[] args)
        {
            // Create a cancellation token to handle termination requests gracefully
            var tokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true; // Prevent the application from terminating immediately
                tokenSource.Cancel();

            };

            Console.WriteLine($"Experiment: {_projectName} has started");

            // Initialize configuration from provided arguments or default sources
            var configurationRoot = InitHelpers.InitConfiguration(args);
            var configurationSection = configurationRoot.GetSection("MyConfig");

            // Initialize logging framework with configurations
            var logFactory = InitHelpers.InitLogging(configurationRoot);
            var logger = logFactory.CreateLogger<AzureStorageProvider>();

            logger?.LogInformation($"{DateTime.Now} - Initialization complete for: {_projectName}");

            // Create an instance of AzureStorageProvider for handling file operations
            var storageProvider = new AzureStorageProvider(configurationSection, logger);

            // Example of downloading an input file to local storage
            var testFileName = ".png";
            try
            {
                logger?.LogInformation($"Starting download of file: {testFileName}");
                var localFilePath = await storageProvider.FetchInputFileAsync(testFileName); 

                if (!string.IsNullOrEmpty(localFilePath))
                {
                    logger?.LogInformation($"File successfully downloaded to: {localFilePath}");
                }
                else
                {
                    logger?.LogWarning($"Failed to download file: {testFileName}");
                }
            }
            catch (Exception ex)
            {
                // Log any errors that occur during the file download process
                logger?.LogError(ex, "Error occurred during file download.");
            }

            // Initialize the experiment processing component
            var experiment = new Experiment(configurationSection, storageProvider, logger);
            
            logger?.LogInformation($"Cancellation token status: {tokenSource.Token.IsCancellationRequested}");

            // Main loop to process incoming experiment requests
            while (!tokenSource.Token.IsCancellationRequested)
            {
                // Retrieve the next experiment request from storage
                var request = await storageProvider.GetExperimentRequestAsync(tokenSource.Token); 

                if (request != null)
                {
                    try
                    {
                        logger?.LogInformation($"Processing experiment request: {request.InputFile}");

                        // Download the input file associated with the request
                        var localFileWithInputArgs = await storageProvider.FetchInputFileAsync(request.InputFile); 
                        if (!string.IsNullOrEmpty(localFileWithInputArgs))
                        {
                            logger?.LogInformation($"Input file downloaded to: {localFileWithInputArgs}");
                        }
                        else
                        {
                            logger?.LogWarning($"Failed to download input file: {request.InputFile}");
                        }

                        // Run the experiment with the provided input file
                        var result = await experiment.RunAsync(localFileWithInputArgs);

                        // Upload the experiment result to a storage file
                        await storageProvider.UploadExperimentResultAsync("outputfile", (IExperimentResult)result);

                        // Optionally, upload the experiment result to a database or table storage
                        await storageProvider.UploadExperimentResultToTableAsync(result);

                        // Commit the experiment request, marking it as processed
                        await storageProvider.SaveExperimentAsync(request);

                    }
                    catch (Exception ex)
                    {
                        // Log any errors that occur during the experiment processing
                        logger.LogError(ex, "Error occurred while processing the request.");
                    }
                }
                else
                {
                    // If no requests are available, wait before checking again
                    await Task.Delay(500); // Delay to avoid tight loop
                    logger?.LogTrace("No requests in the queue.");
                }
            }

            logger?.LogInformation($"{DateTime.Now} - Experiment concluded: {_projectName}");
        }
    }
}