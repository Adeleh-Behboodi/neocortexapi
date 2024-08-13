using MyCloudProject.Common; // یا MyExperiment، بسته به نیاز
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

            Console.WriteLine($"Started experiment: {_projectName}");

            // Init configuration
            var cfgRoot = Common.InitHelpers.InitConfiguration(args);

            var cfgSec = cfgRoot.GetSection("MyConfig");

            // InitLogging
            var logFactory = InitHelpers.InitLogging(cfgRoot);

            var logger = logFactory.CreateLogger<AzureStorageProvider>();

            logger?.LogInformation($"{DateTime.Now} - Started experiment: {_projectName}");

            //var aslogger = (ILogger<AzureStorageProvider>)logger;
            MyExperiment.IStorageProvider storageProvider = new AzureStorageProvider(cfgSec, logger); //as ILogger<AzureStorageProvider>


            IExperiment experiment = new Experiment(cfgSec, storageProvider, logger as ILogger<Experiment>);

            while (!tokeSrc.Token.IsCancellationRequested)
            {
                ExerimentRequest request = await storageProvider.ReceiveExperimentRequestAsync(tokeSrc.Token);

                //var request = await storageProvider.ReceiveExperimentRequestAsync(tokeSrc.Token);

                if (request != null)
                {
                    try
                    {
                        // Step 4.
                        var localFileWithInputArgs = await storageProvider.DownloadInputAsync(request.InputFile);

                        IExperimentResult result = await experiment.RunAsync(localFileWithInputArgs);

                        await storageProvider.UploadResultAsync("outputfile", result);

                        await storageProvider.CommitRequestAsync(request);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "An error occurred while processing the request.");
                    }
                }
                else
                {
                    await Task.Delay(500);
                    logger?.LogTrace("Queue empty...");
                }
            }

            logger?.LogInformation($"{DateTime.Now} - Experiment exit: {_projectName}");
        }
    }
}
