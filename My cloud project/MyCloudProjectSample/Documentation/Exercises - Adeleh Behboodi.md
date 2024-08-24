
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
3- 



# Tools and Platforms Required for the Implementation of the Project:

1- Dockerfile : [link](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/MyCloudProject/Dockerfile)
2-  The docker image : [link](https://blobcontainersub4.blob.core.windows.net/containersub4/testfile.png)
3- The source code: [link](https://github.com/Adeleh-Behboodi/neocortexapi/tree/CodeX)
4- Azure Blob Storage : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.Storage/storageAccounts/blobcontainersub4/keys)
5- The App service : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-Ex2/providers/Microsoft.Web/sites/ccappwebadeleh/appServices)
6- The container registry : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.ContainerRegistry/registries/SUB4CONTREG/overview)
7- The function app : [link](https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.Web/sites/SUB4/appServices)


# Components and application:

#### 1- Source code repository (GitHub): 
A place to store program codes.

#### 2- Docker : 
A place to host Docker images built from source code.
Facilitating the distribution and management of Docker images.
![Fig.1 Building Docker Image](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Docker%20desktop%20-%20Images.png)
![Fig.2 Building Docker Container](https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/Documentation/Docker%20desktop%20-%20containers.png)

#### 3- Azure Container Registry (ACR): 
Secure space to store and manage Docker images in Azure environment.
Suitable for integration with other Azure services.
Azure Container Instances (ACI): Management and implementation of container programs.

#### 4- Azure Container Instances (ACI): 
Management and implementation of container programs.
Ensuring scalable and stable execution of Docker containers.

#### 5- Azure Blob Storage (Input):
Providing unlimited storage space for incoming data. Provide the required input data for the application container.

#### 6- Application container:
Running the application inside a Docker container.
Input data processing and output generation.

#### 7- Azure Table Storage (output):
Store processed data in a structured format without the need for SQL.
Ensuring efficient and scalable data storage and retrieval.

# Conclusion

The Neocortex API project utilizes to Azure Cloud to process image and scalar data based on neocortex principles. Experiments demonstrated its effectiveness in encoding, spatial pooling, and reconstruction of data. Scalar data were encoded and binarized images were processed to capture spatial patterns. Reconstructed outputs showed average similarity to the originals, with images exhibiting more noise than scalar inputs. Increasing the threshold reduced noise but affected image quality. Future work could focus on improving encoding and reconstruction methods and exploring real-world applications like image recognition and anomaly detection.
This method ensures that users can effectively manage and analyze large datasets, allowing them to generate valuable insights and make robust predictions with ease. In addition, this method that facilitates the binarization of images and numbers using neocortex algorithms