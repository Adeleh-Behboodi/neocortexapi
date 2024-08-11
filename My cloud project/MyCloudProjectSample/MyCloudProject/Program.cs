using MyCloudProject.Common;
using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Threading;
using MyExperiment;
using System.Threading.Tasks;
using Azure.Storage.Queues.Models;
using Azure.Storage.Queues;
using Azure.Storage.Blobs;
using System.Text.Json;
using System.Text;
using System.Runtime.CompilerServices;
using System.IO;

namespace MyCloudProject
{
    class Program
    {
        /// <summary>
        /// Your project ID from the last semester.
        /// </summary>
        private static string _projectName = "ML 23/24-4";

        string test;

        private static async Task CreateQueueIfNotExistsAsync()
        {
            string connectionString = "DefaultEndpointsProtocol=https;AccountName=blobcontainersub4;AccountKey=Sd9tYA23WeUFrFwhJwmJFxbrd6vz6JqPQw3PkCGrTSxpKmroHPM0SdWJDYMFkncfDnulp/mhWxnL+AStYSZwGA==;EndpointSuffix=core.windows.net";
            string queueName = "trigger-queue";

            QueueServiceClient queueServiceClient = new QueueServiceClient(connectionString);
            QueueClient queueClient = queueServiceClient.GetQueueClient(queueName);

            await queueClient.CreateIfNotExistsAsync();
        }

        private static async Task SendMessageToQueueAsync()
        {
            string connectionString = "DefaultEndpointsProtocol=https;AccountName=blobcontainersub4;AccountKey=Sd9tYA23WeUFrFwhJwmJFxbrd6vz6JqPQw3PkCGrTSxpKmroHPM0SdWJDYMFkncfDnulp/mhWxnL+AStYSZwGA==;EndpointSuffix=core.windows.net";
            string queueName = "trigger-queue";

            QueueServiceClient queueServiceClient = new QueueServiceClient(connectionString);
            QueueClient queueClient = queueServiceClient.GetQueueClient(queueName); await queueClient.CreateIfNotExistsAsync();

            await queueClient.CreateIfNotExistsAsync();

            string messageContent = JsonSerializer.Serialize(new ExperimentRequest
            {
                InputFile = "test-file.png",
            });

            var base64Message = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageContent));
            await queueClient.SendMessageAsync(base64Message); 
            Console.WriteLine("Message sent to queue.");
        }


        static async Task Main(string[] args)
        {
            CancellationTokenSource tokeSrc = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                tokeSrc.Cancel();
            };

            Console.WriteLine($"Started experiment: Implement the Spatial Pooler SDR Reconstruction.");

            // Init configuration
            var builder = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            var cfgRoot = builder.Build();
            var cfgSec = cfgRoot.GetSection("MyConfig");


            // InitLogging

            var logFactory = InitHelpers.InitLogging(cfgRoot);

            var logger = logFactory.CreateLogger<AzureStorageProvider>(); // Ensure logger is created for AzureStorageProvider
            MyExperiment.IStorageProvider storageProvider = new MyExperiment.AzureStorageProvider(cfgRoot, logger);

            await SendMessageToQueueAsync();
            IExperiment experiment = new Experiment(cfgSec, storageProvider, logger/* put some additional config here */);

            // Implements the step 3 in the architecture picture.

            var maxRetries = 10; // the maximum numbers of attempt
            var retries = 0;

            while (tokeSrc.Token.IsCancellationRequested == false && retries < maxRetries)

            {


                IExperimentRequest request = await storageProvider.ReceiveExperimentRequestAsync(tokeSrc.Token);

                if (request != null)
                {
                    try
                    {

                        // logging

                        // Step 4.

                        if (string.IsNullOrEmpty(request.InputFile))
                        {
                            logger.LogError("Input file is null or empty.");
                            return;
                        }

                        logger.LogInformation($"Starting download of input file: {request.InputFile}");

                        var localFileWithInputArgs = await storageProvider.DownloadInputAsync(request.InputFile);


                        logger.LogInformation($"Downloaded input file to: {localFileWithInputArgs}");

                        // Checking the content and existence of the downloaded file
                        if (File.Exists(localFileWithInputArgs))
                        {
                            logger.LogInformation($"File {localFileWithInputArgs} exists. Checking content...");
                            string fileContent = await File.ReadAllTextAsync(localFileWithInputArgs);

                            logger.LogInformation($"Content of {localFileWithInputArgs}: {fileContent.Substring(0, Math.Min(fileContent.Length, 100))}..."); // نمایش 100 کاراکتر اول
                        }
                        else
                        {
                            logger.LogError($"File {localFileWithInputArgs} does not exist.");
                        }


                        // logging

                        // Here is your SE Project code started.(Between steps 4 and 5).
                        IExperimentResult result = await experiment.RunAsync(localFileWithInputArgs);


                        // logging

                        // Step 5.
                        await storageProvider.UploadResultAsync(request.InputFile, result);

                        // logging

                        await storageProvider.CommitRequestAsync(request);
                        //await queueClient.DeleteMessageAsync(LoggerMessage.MessageId, message.PopReceipt);

                        // loggingx

                        logger.LogInformation("Committed request.");

                    }
                    catch (Exception ex)
                    {
                        // logging

                        logger.LogError(ex, "Error occurred during processing.");
                    }
                }
                else
                {
                    retries++;
                    await Task.Delay(1000);
                    logger?.LogTrace("Queue empty...");
                }
            }

            logger?.LogInformation("Max retries reached or cancellation requested. Exiting loop.");
            logger?.LogInformation($"{DateTime.Now} - Experiment exit: Implement the Spatial Pooler SDR Reconstruction.");
        }
    }
}