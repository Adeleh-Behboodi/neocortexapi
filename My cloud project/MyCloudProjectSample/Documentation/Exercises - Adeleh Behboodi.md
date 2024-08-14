
ML 23/24-4 : Implement the Spatial Pooler SDR Reconstruction - Azure Cloud Implementation

	Last year I started a project that includes Implement the Spatial Pooler SDR Reconstruction with NeocortexAPI and working locally and teamwork.The inspiration for this project comes from the intricate structure and function of the neocortex, which serves as the foundation for the Hierarchical Temporal Memory (HTM) machine learning framework. A key component within HTM is the Spatial Pooler, responsible for generating sparse distributed representations (SDRs) of input data. However, a significant challenge has been reconstructing the original input values from these SDRs. This project introduces a method called the "Reconstructor," designed to accurately reverse the encoding process and recover the original input values from their corresponding SDRs within HTM systems. By leveraging the permanence values produced by the Spatial Pooler, the Reconstructor effectively reconstructs the input data. Through comprehensive experiments and evaluations, the project demonstrates the Reconstructor’s capability in accurately recovering original input values within HTM systems. This work advances HTM technology by providing a reliable approach for reconstructing input data from SDRs, paving the way for improved data processing and pattern recognition applications.
	This year, the project has made significant strides by integrating with cloud-based infrastructure. The primary goal is to scale the project further, harnessing the capabilities of cloud computing to explore new dimensions in machine learning. By leveraging cloud resources, the project can handle larger datasets and deploy more advanced models, thus enhancing the accuracy and efficiency of input reconstruction within HTM systems.

	This transition is not merely about improving performance; it’s about expanding the practical applications of the "Reconstructor" method. By adopting cloud technology, I am moving closer to real-world use cases, such as refining data processing techniques and advancing pattern recognition capabilities.

	Managing this integration individually, I am pushing the boundaries of HTM technology. This effort not only highlights the potential of combining HTM with powerful cloud-based platforms but also offers new opportunities for those interested in the forefront of machine learning.


Objective: 
	The objective is to assess the system's ability to accurately process and reconstruct visual data representations.


Tools and Platforms Required for the Implementation of the Project:

	1- Dockerfile : https://github.com/Adeleh-Behboodi/neocortexapi/blob/CodeX/My%20cloud%20project/MyCloudProjectSample/MyCloudProject/Dockerfile

	2-  The docker image : https://blobcontainersub4.blob.core.windows.net/containersub4/testfile.png
 
	3- The source code: https://github.com/Adeleh-Behboodi/neocortexapi/tree/CodeX

	4- Azure Blob Storage : https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.Storage/storageAccounts/blobcontainersub4/keys

	5- The App service : https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-Ex2/providers/Microsoft.Web/sites/ccappwebadeleh/appServices

	6- The container registry : https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.ContainerRegistry/registries/SUB4CONTREG/overview

	7- The function app : https://portal.azure.com/#@stud.fra-uas.de/resource/subscriptions/5adcff6e-ace2-4012-b13c-dc7a940afff2/resourceGroups/RG-AB-SUB4/providers/Microsoft.Web/sites/SUB4/appServices


Components and application:

	Source code repository (GitHub): A place to store program codes.

	Docker : 
	A place to host Docker images built from source code.
	Facilitating the distribution and management of Docker images.
	E:\Project\neocortexapi\My cloud project\MyCloudProjectSample\Documentation\Docker desktop

	Azure Container Registry (ACR): 
	Secure space to store and manage Docker images in Azure environment.
	Suitable for integration with other Azure services.
	Azure Container Instances (ACI): Management and implementation of container programs.


	Azure Container Instances (ACI): 
	Management and implementation of container programs.
	Ensuring scalable and stable execution of Docker containers.

	Azure Blob Storage (Input):
	Providing unlimited storage space for incoming data. Provide the required input data for the application container.

	Application container:
	Running the application inside a Docker container.
	Input data processing and output generation.

	Azure Table Storage (output):
	Store processed data in a structured format without the need for SQL.
	Ensuring efficient and scalable data storage and retrieval.




- Output
Contains output of the traned models

- 'Test' for playing and testing.
you should provide here SAS Url to 2-3 files in this container with time expire 1 year.

#### Exercise 6 - Table Storage

Provide us access to the account which you have used for table exersises.

#### Exercise 7 - Queue Storage

Provide us access to the account which you have used for queue exersises.
