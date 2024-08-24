using LearningFoundation;
using NeoCortexApi;
using NeoCortexApi.Encoders;
using NeoCortexApi.Entities;
using NeoCortexApi.Network;
using NeoCortexApi.Utility;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;

namespace NeoCortexApiSample
{
    /// <summary>
    /// Demonstrates how to use the Spatial Pooler for learning spatial patterns from input data.
    /// The Spatial Pooler (SP) learns patterns over multiple iterations to capture the structure of the input data.
    /// </summary>
    public class SpatialPatternLearning
    {
        /// <summary>
        /// Executes the spatial pattern learning experiment based on the specified parameters.
        /// </summary>
        /// <param name="experimentId">Identifier for selecting the type of experiment.</param>
        /// <param name="maxValue">Maximum value for scaling inputs in the experiment.</param>
        /// <param name="inputFolderLocation">Path to the folder containing input image files for the experiment.</param>
        public void Run(string experimentId, string maxValue, string inputFolderLocation)
        {
            Console.WriteLine($"Starting experiment: {nameof(SpatialPatternLearning)}");

            // Configuration for the Hierarchical Temporal Memory (HTM) model
            double maxBoost = 5.0;
            int inputBits = 200;
            int numColumns = 1024;

            var config = new HtmConfig(new int[] { inputBits }, new int[] { numColumns })
            {
                CellsPerColumn = 10,
                MaxBoost = maxBoost,
                DutyCyclePeriod = 100,
                MinPctOverlapDutyCycles = 1.0,
                GlobalInhibition = false,
                NumActiveColumnsPerInhArea = 0.02 * numColumns,
                PotentialRadius = (int)(0.15 * inputBits),
                LocalAreaDensity = -1,
                ActivationThreshold = 10,
                MaxSynapsesPerSegment = (int)(0.01 * numColumns),
                Random = new ThreadSafeRandom(42),
                StimulusThreshold = 10,
            };

            double max = Convert.ToDouble(maxValue);
            Console.WriteLine("Processing... Please wait.");

            switch (Convert.ToDouble(experimentId))
            {
                case 1:
                    var encoder = CreateScalarEncoder(max, inputBits);
                    var inputValues = Enumerable.Range(0, (int)max).Select(i => (double)i).ToList();

                    var spatialPooler = RunSpatialPoolerExperiment(config, encoder, inputValues);
                    RunRestructuringExperiment(spatialPooler, encoder, inputValues);
                    break;

                case 2:
                    // Placeholder for additional experiments
                    Console.WriteLine("Experiment ID 2 is not yet implemented.");
                    break;

                default:
                    Console.WriteLine("Invalid experiment ID.");
                    break;
            }
        }

        /// <summary>
        /// Creates a ScalarEncoder with specified settings for encoding input values.
        /// </summary>
        /// <param name="max">Maximum value to scale inputs.</param>
        /// <param name="inputBits">Number of bits representing the input values.</param>
        /// <returns>Configured ScalarEncoder instance.</returns>
        private static EncoderBase CreateScalarEncoder(double max, int inputBits)
        {
            var settings = new Dictionary<string, object>
            {
                { "W", 15 },
                { "N", inputBits },
                { "Radius", -1.0 },
                { "MinVal", 0.0 },
                { "Periodic", false },
                { "Name", "scalar" },
                { "ClipInput", false },
                { "MaxVal", max }
            };

            return new ScalarEncoder(settings);
        }

        /// <summary>
        /// Runs the spatial pooler experiment to learn spatial patterns from input values.
        /// </summary>
        /// <param name="config">Configuration settings for the HTM model.</param>
        /// <param name="encoder">Encoder used for transforming input values into a format suitable for the Spatial Pooler.</param>
        /// <param name="inputValues">List of input values used for training the Spatial Pooler.</param>
        /// <returns>Trained SpatialPooler instance.</returns>
        private static SpatialPooler RunSpatialPoolerExperiment(HtmConfig config, EncoderBase encoder, List<double> inputValues)
        {
            var memory = new Connections(config);
            var homeostaticController = new HomeostaticPlasticityController(memory, inputValues.Count,
                (isStable, numPatterns, actColAvg, seenInputs) =>
                {
                    if (!isStable)
                    {
                        Debug.WriteLine("System is in an unstable state.");
                    }
                    else
                    {
                        Debug.WriteLine("System has reached a stable state.");
                    }
                });

            var spatialPooler = new SpatialPooler(homeostaticController);
            spatialPooler.Init(memory, new DistributedMemory { ColumnDictionary = new InMemoryDistributedDictionary<int, Column>(1) });
            var cortexLayer = new CortexLayer<object, object>("Layer");

            cortexLayer.HtmModules.Add("encoder", encoder);
            cortexLayer.HtmModules.Add("spatialPooler", spatialPooler);

            var inputs = inputValues.ToArray();
            var previousActiveColumns = inputs.ToDictionary(input => input, input => new int[0]);
            var previousSimilarity = inputs.ToDictionary(input => input, input => 0.0);

            const int maxCycles = 1000;
            int stableCycleCount = 0;
            bool isInStableState = false;

            for (int cycle = 0; cycle < maxCycles; cycle++)
            {
                Debug.WriteLine($"Cycle {cycle} - Stability: {isInStableState}");

                foreach (var input in inputs)
                {
                    var encodedInput = cortexLayer.Compute((object)input, true) as int[];
                    var activeColumns = cortexLayer.GetResult("spatialPooler") as int[];

                    double similarity = MathHelpers.CalcArraySimilarity(activeColumns, previousActiveColumns[input]);
                    Debug.WriteLine($"Cycle={cycle:D4}, Input={input}, Columns={activeColumns.Length}, Similarity={similarity}%");

                    previousActiveColumns[input] = activeColumns;
                    previousSimilarity[input] = similarity;
                }

                if (isInStableState)
                {
                    stableCycleCount++;
                }

                if (stableCycleCount > 5)
                {
                    break;
                }
            }
            return spatialPooler;
        }

        /// <summary>
        /// Analyzes the reconstruction of input values using the trained Spatial Pooler and saves the results.
        /// </summary>
        /// <param name="spatialPooler">Spatial Pooler used for analyzing input values.</param>
        /// <param name="encoder">Encoder used for encoding input values.</param>
        /// <param name="inputValues">List of input values to be processed.</param>
        private void RunRestructuringExperiment(SpatialPooler spatialPooler, EncoderBase encoder, List<double> inputValues)
        {
            const string outputFolder = "RestructuringExperimentResults";
            if (Directory.Exists(outputFolder))
            {
                Directory.Delete(outputFolder, true);
            }
            Directory.CreateDirectory(outputFolder);

            foreach (var input in inputValues)
            {
                var encodedInput = encoder.Encode(input);
                int[] reconstructedValues = AnalyzeReconstruction(spatialPooler, encodedInput);

                double similarity = CalculateSimilarity(encodedInput, reconstructedValues);
                Console.WriteLine($"Similarity for input {input}: {similarity}%");

                SaveReconstructedImage(reconstructedValues, similarity, input, outputFolder);
            }
        }
        
        
        /// <summary>
        /// Analyzes the reconstruction of encoded input data using the Spatial Pooler.
        /// </summary>
        /// <param name="spatialPooler">Spatial Pooler used for reconstruction.</param>
        /// <param name="encodedInput">Encoded input data.</param>
        /// <returns>Thresholded reconstruction values.</returns>
        private static int[] AnalyzeReconstruction(SpatialPooler spatialPooler, int[] encodedInput)
        {
            var activeColumns = spatialPooler.Compute(encodedInput, false);
            var probabilities = spatialPooler.Reconstruct(activeColumns);
            var normalizedData = Normalize(probabilities);

            const double threshold = 0.6;
            return ApplyThreshold(normalizedData, threshold);
        }

        /// <summary>
        /// Calculates the similarity percentage between the original and reconstructed data.
        /// </summary>
        /// <param name="original">Original input data.</param>
        /// <param name="reconstructed">Reconstructed data after processing.</param>
        /// <returns>Similarity percentage between the original and reconstructed data.</returns>
        private static double CalculateSimilarity(int[] original, int[] reconstructed)
        {
            int matchingCount = original.Zip(reconstructed, (o, r) => o == r ? 1 : 0).Sum();
            return Math.Round((double)matchingCount / original.Length * 100, 2);
        }

        /// <summary>
        /// Saves the reconstructed image as a PNG file.
        /// </summary>
        /// <param name="thresholdValues">Reconstructed image data after applying threshold.</param>
        /// <param name="similarity">Similarity percentage of the reconstruction.</param>
        /// <param name="input">Input value used for naming the output file.</param>
        /// <param name="outputFolder">Path to the folder where images are saved.</param>
        private void SaveReconstructedImage(int[] thresholdValues, double similarity, double input, string outputFolder)
        {
            int width = (int)Math.Sqrt(thresholdValues.Length);
            int height = width;

            using (var bitmap = new Bitmap(width, height))
            {
                for (int y = 0; y < height; y++)
                {
                    for (int x = 0; x < width; x++)
                    {
                        int value = thresholdValues[y * width + x] > 0 ? 255 : 0;
                        bitmap.SetPixel(x, y, Color.FromArgb(value, value, value));
                    }
                }

                string fileName = Path.Combine(outputFolder, $"Reconstruction_{input}_Similarity_{similarity}.png");
                bitmap.Save(fileName);
                Console.WriteLine($"Saved reconstructed image for input {input} with similarity {similarity}%.");
            }
        }

        /// <summary>
        /// Normalizes the reconstructed data to a range of 0 to 1.
        /// </summary>
        /// <param name="probabilities">Reconstructed data values.</param>
        /// <returns>Normalized data as an array of doubles.</returns>
        private static double[] Normalize(Dictionary<int, double> probabilities)
        {
            var max = probabilities.Values.Max();
            var min = probabilities.Values.Min();
            var range = max - min;

            return probabilities.Values
                .Select(value => range == 0 ? 0 : (value - min) / range)
                .ToArray();
        }

        /// <summary>
        /// Applies a threshold to the data to binarize it.
        /// </summary>
        /// <param name="data">Data values to be thresholded.</param>
        /// <param name="threshold">Threshold value to apply.</param>
        /// <returns>Binarized data array.</returns>
        private static int[] ApplyThreshold(double[] data, double threshold)
        {
            return data.Select(value => value > threshold ? 1 : 0).ToArray();
        }
    }
}