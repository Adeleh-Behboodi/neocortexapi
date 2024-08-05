using MyCloudProject.Common;
using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Threading;
using MyExperiment;
using System.Threading.Tasks;
using Azure.Storage.Queues.Models;
using Azure.Storage.Queues;
using System.Text.Json;
using System.Text;

namespace MyCloudProject
{
    class Program
    {
        /// <summary>
        /// Your project ID from the last semester.
        /// </summary>
        private static string _projectName = "ML 23/24-4";

        string test;

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
            var cfgRoot = Common.InitHelpers.InitConfiguration(args);

            var cfgSec = cfgRoot.GetSection("MyConfig");

            // InitLogging
            var logFactory = InitHelpers.InitLogging(cfgRoot);
          
            var logger = logFactory.CreateLogger("Train.Console");

            logger?.LogInformation($"{DateTime.Now} - Started experiment: Implement the Spatial Pooler SDR Reconstruction.");

            IStorageProvider storageProvider = new AzureStorageProvider(cfgSec);

            IExperiment experiment = new Experiment(cfgSec, storageProvider, logger/* put some additional config here */);

            //
            // Implements the step 3 in the architecture picture.
            while (tokeSrc.Token.IsCancellationRequested == false)
            {
                // Step 3
                IExerimentRequest request = storageProvider.ReceiveExperimentRequestAsync(tokeSrc.Token);

                if (request != null)
                {
                    try
                    {

                        // logging
                        logger.LogInformation("Processing request."); // me

                        // Step 4.
                        var localFileWithInputArgs = await storageProvider.DownloadInputAsync(request.InputFile);


                        // logging
                        logger.LogInformation("Downloaded input file: {InputFileName}", request.InputFile); // me


                        // Here is your SE Project code started.(Between steps 4 and 5).
                        IExperimentResult result = await experiment.RunAsync(localFileWithInputArgs);


                        // logging
                        logger.LogInformation("Experiment run completed with result: {Result}", result); // me


                        // Step 5.
                        await storageProvider.UploadResultAsync("outputfile", result);
                        logger.LogInformation("Uploaded result file: outputfile"); // me


                        // logging

                        await storageProvider.CommitRequestAsync(request);

                        // loggingx

                        logger.LogInformation("Committed request.");  // me


                    }
                    catch (Exception ex)
                    {
                        // logging
                    }
                }
                else
                {
                    await Task.Delay(500);
                    logger?.LogTrace("Queue empty...");
                }
            }

            logger?.LogInformation($"{DateTime.Now} - Experiment exit: Implement the Spatial Pooler SDR Reconstruction.");
        }


    }
}
