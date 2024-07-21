Spatial Pattern Learning with NeoCortexApi
This repository contains a C# implementation of an experiment demonstrating spatial pattern learning using the NeoCortexApi library. The experiment involves learning spatial patterns from input data, either integers or images, and analyzing the similarity between original and reconstructed patterns.

Features
Learn spatial patterns from integer or image inputs using the Hierarchical Temporal Memory (HTM) model.
Run restructuring experiments to analyze the learned patterns.
Compute similarity metrics between original and reconstructed patterns.
Support for both integer and image inputs.

Getting Started:
To run the experiment, follow these steps:

1)Clone this repository to your local machine.

2)Ensure you have the necessary dependencies installed, including:

Daenet.ImageBinarizerLib
LearningFoundation
NeoCortex
3)Open the solution in your preferred C# IDE.

4)Build the solution to ensure all dependencies are resolved.

5)Before running the experiment, make sure to check for the input image path and specify the output folder where the generated images will be saved.

	Input Image Path: Ensure that the path to the input image is correctly specified in the code. This is typically set in the BinarImage().
	Output Folder: Specify the path to the folder where the generated images will be saved.This is typically set in the BinarImage().
	Bitmap Output Path: The bitmap images generated during the experiment will be saved in the output folder specified. You can find the generated bitmap images in the below mentioned folder after running the experiment.
	neocortexapi\source\Samples\NeoCortexApiSample\bin\Debug\net8.0\RunRustructuringExperiment.
6)Run the SpatialPatternLearning class to start the experiment.

7)Follow the on-screen prompts to choose between integer and image inputs.

8)Review the output to see the results of the experiment, including similarity metrics.

Usage:
SpatialPatternLearning.cs: Contains the main experiment implementation.
NeoCortexUtils.cs: Utility methods for drawing bitmaps and handling binary data.
BinarImage(): Helper method for binarizing images and reading binary data.

Dependencies:
Daenet.ImageBinarizerLib
LearningFoundation
NeoCortex


Acknowledgments:
This experiment is based on the NeoCortexApi library developed by NeoCortexApi.
Special thanks to the developers and contributors of the NeoCortexApi library.
Contributing
Contributions are welcome! Please feel free to open issues or submit pull requests to improve this project.