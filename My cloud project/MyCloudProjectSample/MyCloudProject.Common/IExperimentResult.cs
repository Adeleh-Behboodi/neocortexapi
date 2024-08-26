
using System;
using System.Collections.Generic;
using System.Text;

namespace MyCloudProject.Common
{
    public interface IExperimentResult
    {
        string ExperimentId { get; set; } 
        string InputFileUrl { get; set; }
        DateTime? StartTimeUtc { get; set; }
        DateTime? EndTimeUtc { get; set; }
        TimeSpan Duration { get; set; }
    }
}
