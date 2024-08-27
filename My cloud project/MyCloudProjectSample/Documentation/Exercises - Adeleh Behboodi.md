
# ML 23/24-4 : Implement the Spatial Pooler SDR Reconstruction - Azure Cloud Implementation
-----------------------------------------------------------------------------------------

# Introduction 

Last year, I started a project to implement Spatial Pooler SDR Reconstruction using the Neocortex API. Inspired by the neocortex and the Hierarchical Temporal Memory (HTM) framework, the project aimed to reconstruct original input values from Sparse Distributed Representations (SDRs).
I introduced the "Reconstructor," a method to reverse the encoding process and accurately recover original inputs from SDRs using permanence values. This advancement has improved HTM technology and data processing.
This year, the project has integrated with cloud infrastructure to scale up, handle larger datasets, and deploy more advanced models. I am using the Neocortex API to process scalar data and images, enhancing the "Reconstructor" method and exploring new machine learning applications.

This transition is not merely about improving performance; it’s about expanding the practical applications of the "Reconstructor" method. By adopting cloud technology, I am moving closer to real-world use cases, such as refining data processing techniques and advancing pattern recognition capabilities.
Managing this integration individually, I am pushing the boundaries of HTM technology. This effort not only highlights the potential of combining HTM with powerful cloud-based platforms but also offers new opportunities for those interested in the forefront of machine learning.

# Objective: 

The objective is to assess the system's ability to accurately process and reconstruct visual data representations.


# Links related to cloud projects

1- My Experiment [link](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/MyExperiment/MyExperiment.csproj)

2- SpartialPatternLearning.cs [link](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/MyExperiment/SpartialPatternLearning.cs) 


# Tools and Platforms Required for the Implementation of the Project:

1- Dockerfile : [link](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/MyCloudProject/Dockerfile)

2-  The docker image : [link](https://blobcontainersub4.blob.core.windows.net/containersub4/testfile.png)

3- The source code: [link](https://github.com/Adeleh-Behboodi/neocortexapi/tree/CodeX)

4- Azure Blob Storage : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.Storage/storageAccounts/blobcontainersub4/keys)

5- The App service : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-Ex2/providers/Microsoft.Web/sites/ccappwebadeleh/appServices)

6- The container registry : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.ContainerRegistry/registries/SUB4CONTREG/overview)

7- The function app : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.Web/sites/SUB4/appServices)


# Components and application:

### 1- Source code repository (GitHub): 
A place to store program codes.


### 2- Docker : 
A place to host Docker images built from source code.
Facilitating the distribution and management of Docker images.

![Fig.1](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Docker%20desktop%20-%20Images.png)
Fig.1 Building Docker Image


![Fig.2](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Docker%20desktop%20-%20containers.png)
Fig.2 Building Docker Container


### 3- Azure Container Registry (ACR): 
Secure space to store and manage Docker images in Azure environment.
Suitable for integration with other Azure services.
Azure Container Instances (ACI): Management and implementation of container programs.

![Fig.3](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Azure%20Resource%20Group%20Overview.png)
Fig.3 : Azure Resource group 


### 4- Azure Container Instances (ACI): 
Management and implementation of container programs.
Ensuring scalable and stable execution of Docker containers.

![Fig.4](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Azure%20container%20instances.png)
Fig.4 : Azure Container Instance


### 5- Azure Blob Storage (Input and out output):
Providing unlimited storage space for incoming data. Provide the required input data for the application container.

![Fig.5](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Azure%20Storage%20account%20-%20Containers%20.png)
Fig.5 : Azure Storage account 


### 6- Application container:
Running the application inside a Docker container.
Input data processing and output generation.

![Fig.6](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Azure%20Storage%20account.png)
Fig.6 : Azure Storage account - overview


![Fig.7](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/containersub4.png)
Fig.7 : Input file

![Fig.8](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/outputfile.png)
Fig.8 : output file


### 7- Azure Table Storage :
Store processed data in a structured format without the need for SQL.
Ensuring efficient and scalable data storage and retrieval.

![Fig.9](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/trigger-queue.png)
Fig.9 : trigger-queue

![Fig.10](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Message1-2%20%20in%20queue.png)
![Fig.10](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Message3-4%20%20in%20queue.png)
Fig.8 : Messages 

![Fig.11](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/table.png)
Fig.11 : tables 


# Conclusion

The Neocortex API project leverages Azure Cloud to process image and scalar data based on neocortex principles. Experiments involved encoding scalar data and binarizing images to capture spatial patterns, with the results showing that reconstructed images had more noise compared to scalar data. Increasing the threshold reduced noise but compromised image quality. The training process for the spatial pooler continued until stability was achieved, overseen by the HomeostaticPlasticityController (HPC) class. 
Future work could focus on enhancing encoding and reconstruction methods and exploring practical applications such as image recognition and anomaly detection.