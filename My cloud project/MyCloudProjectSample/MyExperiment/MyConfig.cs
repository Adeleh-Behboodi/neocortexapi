using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace MyExperiment
{
    public class MyConfig
    {
        public string StorageConnectionString { get; set; }
        public string AzureBlobStorageConnectionString { get; set; }
        public string AzureQueueStorageConnectionString { get; set; }
        public string AzureTableStorageConnectionString { get; set; }
        public string TrainingContainer { get; set; }
        public string ResultContainer { get; set; }
        public string ResultTable { get; set; }
        public string Queue { get; set; }
        public string GroupId { get; set; }

    }
}