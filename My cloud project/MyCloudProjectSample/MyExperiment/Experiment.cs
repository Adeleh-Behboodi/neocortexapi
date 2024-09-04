using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MyCloudProject.Common;
using NeoCortexApiSample;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MyExperiment
{
    /// <summary>
    /// This class implements the ML experiment that will run in the cloud. This is refactored code from my SE project.
    /// </summary>
    public class Experiment : IExperiment
    {
        private IStorageProvider storageProvider;

        private ILogger logger;

        private MyConfig config;
        private IConfigurationSection configurationSection;
        private ExperimentProcessing.AzureStorageProvider storageProvider1;
        private readonly ILogger<Experiment> _logger;

        public Experiment(IConfigurationSection configSection, IStorageProvider storageProvider, ILogger log)
        {
            this.storageProvider = storageProvider;
            this.logger = log;

            config = new MyConfig();
            configSection.Bind(config);
        }

        public Experiment(IConfigurationSection configurationSection, ExperimentProcessing.AzureStorageProvider storageProvider1, ILogger<ExperimentProcessing.AzureStorageProvider> logger)
        {
            this.configurationSection = configurationSection;
            this.storageProvider1 = storageProvider1;
            this.logger = logger;
            config = new MyConfig();
        }

        /// <summary>
        /// Runs the experiment asynchronously using the provided input data.
        /// </summary>
        /// <param name="inputData">Input data required to run the experiment.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        public async Task<IExperimentResult> RunAsync(ExerimentRequest inputData)
        {
            // Log the start of the experiment
            logger?.LogInformation("Starting the initialization process for SpatialPatternLearning experiment. Preparing required resources and settings.");
            
            // Initialize the experiment instance
            SpatialPatternLearning experiment1 = new SpatialPatternLearning();

            // Run the experiment using the provided input data
            try
            {
                experiment1.Run(inputData.ExperimentId, inputData.MaxValue, inputData.InputFileUrl);
            }
            catch (Exception ex)
            {
                logger?.LogError($"Error encountered during experiment execution. Details: {ex.Message}. StackTrace: {ex.StackTrace}");
                throw;
            }

            // Create the experiment result
            ExperimentResult res = new ExperimentResult(this.config.GroupId, null)
            {
                StartTimeUtc = DateTime.UtcNow,
                OutputFile = Path.Combine(Directory.GetCurrentDirectory(), "RunRustructuringExperiment")
            };

            logger?.LogInformation("SpatialPatternLearning experiment completed.");

            return await Task.FromResult<IExperimentResult>(res);
        }

        public Task<IExperimentResult> RunAsync(string inputData)
        {
            throw new NotImplementedException();
        }
    }
}
