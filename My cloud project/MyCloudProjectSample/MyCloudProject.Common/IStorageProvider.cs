using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyCloudProject.Common
{
    /// <summary>
    /// Defines the contract for all storage operations.
    /// </summary>
    public interface IStorageProvider
    {
        ///// <summary>
        ///// Receives the next message from the queue.
        ///// </summary>
        ///// <param name="token"></param>
        ///// <returns>NULL if there are no messages in the queue.</returns>
        //ExerimentRequest ReceiveExperimentRequestAsync(CancellationToken token);

        ///// <summary>
        ///// Downloads the input file for training. This file contains all required input for the experiment.
        ///// The file is stored in the cloud or any other kind of store or database.
        ///// </summary>
        ///// <param name="fileName">The name of the file at some remote (cloud) location from where the file will be downloaded.</param>
        ///// <returns>The fullpath name of the file as downloaded locally.</returns>
        ///// <remarks>See step 4 in the architecture picture.</remarks>
        //Task<string> DownloadInputAsync(string fileName);

        ///// <summary>
        ///// Uploads the result of the experiment in the cloud or any other kind of store or database.
        ///// </summary>
        ///// <param name="experimentName">The name of the experiment at the remote (cloud) location. The operation creates the file with the name of experiment.</param>
        ///// <param name="result">The result of the experiment that should be uploaded to the table.</param>
        ///// <remarks>See step 5 (oposite way) in the architecture picture.</remarks>
        //Task UploadResultAsync(string experimentName, IExperimentResult result);

        ///// <summary>
        ///// Makes sure that the message is deleted from the queue.
        ///// </summary>
        ///// <param name="request">The requests received by <see cref="nameof(IStorageProvider.ReceiveExperimentRequestAsync)"/>.</param>
        ///// <returns></returns>
        //Task CommitRequestAsync(ExerimentRequest request);

        /// <summary>
        /// Saves the experiment request.
        /// </summary>
        /// <param name="request">The experiment request to save.</param>
        Task SaveExperimentAsync(ExerimentRequest request);

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
        Task<ExerimentRequest> GetExperimentRequestAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Uploads the experiment result to blob storage.
        /// </summary>
        /// <param name="experimentId">ID of the experiment.</param>
        /// <param name="result">Result object to upload.</param>
        Task UploadExperimentResultAsync(string experimentId, IExperimentResult result);

        /// <summary>
        /// Saves an experiment result to Azure Table Storage. 
        /// Ensures the table exists, then adds or updates the experiment's details 
        /// like ID, start time, end time, and duration in the table.
        /// </summary>
        /// <param name="results">The experiment result to save.</param>
        Task UploadExperimentResultToTableAsync(IExperimentResult results);


    }
}
