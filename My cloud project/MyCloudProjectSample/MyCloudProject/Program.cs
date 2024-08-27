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
            var tokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true; // Prevent the application from terminating immediately
                tokenSource.Cancel();
            };

            Console.WriteLine($"Experiment: {_projectName} has started");

            // Initialize configuration
            var configurationRoot = InitHelpers.InitConfiguration(args);
            var configurationSection = configurationRoot.GetSection("MyConfig");

            // Initialize logging
            var logFactory = InitHelpers.InitLogging(configurationRoot);
            var logger = logFactory.CreateLogger<AzureStorageProvider>();

            logger?.LogInformation($"{DateTime.Now} - Initialization complete for: {_projectName}");

            // Create the storage provider instance
            var storageProvider = new AzureStorageProvider(configurationSection, logger);

            // Try downloading an input file
            var testFileName = ".png";
            try
            {
                logger?.LogInformation($"Starting download of file: {testFileName}");
                var localFilePath = await storageProvider.FetchInputFileAsync(testFileName); // اصلاح نام متد

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
                logger?.LogError(ex, "Error occurred during file download.");
            }

            // Initialize experiment processing

            var experiment = new Experiment(configurationSection, storageProvider, logger);
            
            logger?.LogInformation($"Cancellation token status: {tokenSource.Token.IsCancellationRequested}");

            // Main loop to handle experiment requests
            while (!tokenSource.Token.IsCancellationRequested)
            {
                var request = await storageProvider.GetExperimentRequestAsync(tokenSource.Token); 

                if (request != null)
                {
                    try
                    {
                        logger?.LogInformation($"Processing experiment request: {request.InputFile}");

                        var localFileWithInputArgs = await storageProvider.FetchInputFileAsync(request.InputFile); // اصلاح نام متد

                        if (!string.IsNullOrEmpty(localFileWithInputArgs))
                        {
                            logger?.LogInformation($"Input file downloaded to: {localFileWithInputArgs}");
                        }
                        else
                        {
                            logger?.LogWarning($"Failed to download input file: {request.InputFile}");
                        }

                        var result = await experiment.RunAsync(localFileWithInputArgs);

                        await storageProvider.UploadExperimentResultAsync("outputfile", (IExperimentResult)result);

                        await storageProvider.UploadExperimentResultToTableAsync(result);

                        await storageProvider.SaveExperimentAsync(request);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error occurred while processing the request.");
                    }
                }
                else
                {
                    await Task.Delay(500); // Delay to avoid tight loop
                    logger?.LogTrace("No requests in the queue.");
                }
            }

            logger?.LogInformation($"{DateTime.Now} - Experiment concluded: {_projectName}");
        }
    }
}