using System;
using System.Collections.Generic;
using System.Text;

namespace MyCloudProject.Common
{
    /// <summary>
    /// Defines the contract for the message request that will run your experiment.
    /// </summary>
    public class ExerimentRequest
    {
        public string ExperimentId { get; set; }
        public string InputFile { get; set; }
        public string InputFileUrl { get; set; }
        public string Name { get; set; }
        public DateTime? StartTimeUtc { get; set; }  // باید public باشد
        public DateTime? EndTimeUtc { get; set; }
        public string Description { get; set; }
        public string MessageId { get; set; }
        public string FileContentBase64 { get; set; }
        public string MessageReceipt { get; set; }
        public TimeSpan Duration
        {
            get
            {
                if (StartTimeUtc.HasValue && EndTimeUtc.HasValue)
                {
                    return EndTimeUtc.Value - StartTimeUtc.Value;
                }
                return TimeSpan.Zero;
            }
        }

        public ExerimentRequest() { }

        public ExerimentRequest(string experimentId, string inputFileUrl, DateTime? startTimeUtc, DateTime? endTimeUtc)
        {
            ExperimentId = experimentId;
            InputFileUrl = inputFileUrl;
            StartTimeUtc = startTimeUtc;
            EndTimeUtc = endTimeUtc;
        }

    }
}