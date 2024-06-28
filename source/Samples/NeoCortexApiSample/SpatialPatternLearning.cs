using Daenet.ImageBinarizerLib;
using Daenet.ImageBinarizerLib.Entities;
using LearningFoundation;
using NeoCortex;
using NeoCortexApi;
using NeoCortexApi.Encoders;
using NeoCortexApi.Entities;
using NeoCortexApi.Network;
using NeoCortexApi.Utility;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;


namespace NeoCortexApiSample
{
    /// <summary>
    /// Implements an experiment that demonstrates how to learn spatial patterns.
    /// SP will learn every presented input in multiple iterations.
    /// </summary>
    public class SpatialPatternLearning
    {
        public void Run()
        {
            Console.WriteLine($"Hello NeocortexApi! Experiment {nameof(SpatialPatternLearning)}");

            // Used as a boosting parameters
            // that ensure homeostatic plasticity effect.
            double minOctOverlapCycles = 1.0;
            double maxBoost = 5.0;

            // We will use 200 bits to represent an input vector (pattern).
            int inputBits = 200;

            // We will build a slice of the cortex with the given number of mini-columns
            int numColumns = 1024;

            // This is a set of configuration parameters used in the experiment.
            HtmConfig cfg = new HtmConfig(new int[] { inputBits }, new int[] { numColumns })
            {
                CellsPerColumn = 10,
                MaxBoost = maxBoost,
                DutyCyclePeriod = 100,
                MinPctOverlapDutyCycles = minOctOverlapCycles,

                GlobalInhibition = false,
                NumActiveColumnsPerInhArea = 0.02 * numColumns,
                PotentialRadius = (int)(0.15 * inputBits),
                LocalAreaDensity = -1,
                ActivationThreshold = 10,

                MaxSynapsesPerSegment = (int)(0.01 * numColumns),
                Random = new ThreadSafeRandom(42),
                StimulusThreshold = 10,
            };
            double max = 10;
            Console.WriteLine("***** Please enter 1 for Integer,2 for Image *****");
            var x = Console.ReadLine(); // for the user to give the type of input
            Console.WriteLine("***** Please wait for a while...");

            // If condition to check whether the input is integer or image 

            if (x == "1")
            {

                //
                // This dictionary defines a set of typical encoder parameters.
                Dictionary<string, object> settings = new Dictionary<string, object>()
                {
                { "W", 15},
                { "N", inputBits},
                { "Radius", -1.0},
                { "MinVal", 0.0},
                { "Periodic", false},
                { "Name", "scalar"},
                { "ClipInput", false},
                { "MaxVal", max}
                };


                EncoderBase encoder = new ScalarEncoder(settings);

                //
                // We create here 100 random input values.
                List<double> inputValues = new List<double>();

                for (int i = 0; i < (int)max; i++)
                {
                    inputValues.Add((double)i);
                }

                var sp = RunExperiment(cfg, encoder, inputValues);

                RunRustructuringExperiment(sp, encoder, inputValues);
            }
            else if (x == "2")
            {
                int inputBits1 = 17160;
                // This dictionary defines a set of typical encoder parameters.
                Dictionary<string, object> settings = new Dictionary<string, object>()
                {
                { "W", 15},
                { "N", inputBits1},
                { "Radius", -1.0},
                { "MinVal", 0.0},
                { "Periodic", false},
                { "Name", "scalar"},
                { "ClipInput", false},
                { "MaxVal", 10.0}
                };

                HtmConfig cfg1 = new HtmConfig(new int[] { inputBits1 }, new int[] { numColumns })
                {
                    CellsPerColumn = 10,
                    MaxBoost = maxBoost,
                    DutyCyclePeriod = 100,
                    MinPctOverlapDutyCycles = minOctOverlapCycles,

                    GlobalInhibition = false,
                    NumActiveColumnsPerInhArea = 0.02 * numColumns,
                    PotentialRadius = (int)(0.15 * inputBits1),
                    LocalAreaDensity = -1,
                    ActivationThreshold = 10,

                    MaxSynapsesPerSegment = (int)(0.01 * numColumns),
                    Random = new ThreadSafeRandom(42),
                    StimulusThreshold = 10,
                };

                EncoderBase encoder1 = new ScalarEncoder(settings);

                //
                // We create here 100 random input values.
                List<double> inputValues1 = new List<double>();
                var sp1 = RunExperiment(cfg1, encoder1, inputValues1);
                RunRustructuringExperimentImage(sp1);
            }
            else
            {
                Console.WriteLine("Invalid Input");
            }
        }

        /// <summary>
        /// Implements the experiment.
        /// </summary>
        /// <param name="cfg"></param>
        /// <param name="encoder"></param>
        /// <param name="inputValues"></param>
        /// <returns>The trained bersion of the SP.</returns>
        private static SpatialPooler RunExperiment(HtmConfig cfg, EncoderBase encoder, List<double> inputValues)
        {
            // Creates the htm memory.
            var mem = new Connections(cfg);

            bool isInStableState = false;

            //
            // HPC extends the default Spatial Pooler algorithm.
            // The purpose of HPC is to set the SP in the new-born stage at the begining of the learning process.
            // In this stage the boosting is very active, but the SP behaves instable. After this stage is over
            // (defined by the second argument) the HPC is controlling the learning process of the SP.
            // Once the SDR generated for every input gets stable, the HPC will fire event that notifies your code
            // that SP is stable now.
            HomeostaticPlasticityController hpa = new HomeostaticPlasticityController(mem, inputValues.Count * 1,
                (isStable, numPatterns, actColAvg, seenInputs) =>
                {
                    // Event should only be fired when entering the stable state.
                    // Ideal SP should never enter unstable state after stable state.
                    if (isStable == false)
                    {
                        Debug.WriteLine($"INSTABLE STATE");
                        // This should usually not happen.
                        isInStableState = false;
                    }
                    else
                    {
                        Debug.WriteLine($"STABLE STATE");
                        // Here you can perform any action if required.
                        isInStableState = true;
                    }
                });

            // It creates the instance of Spatial Pooler Multithreaded version.
            SpatialPooler sp = new SpatialPooler(hpa);
            //sp = new SpatialPoolerMT(hpa);

            // Initializes the 
            sp.Init(mem, new DistributedMemory() { ColumnDictionary = new InMemoryDistributedDictionary<int, NeoCortexApi.Entities.Column>(1) });

            // mem.TraceProximalDendritePotential(true);

            // It creates the instance of the neo-cortex layer.
            // Algorithm will be performed inside of that layer.
            CortexLayer<object, object> cortexLayer = new CortexLayer<object, object>("L1");

            // Add encoder as the very first module. This model is connected to the sensory input cells
            // that receive the input. Encoder will receive the input and forward the encoded signal
            // to the next module.
            cortexLayer.HtmModules.Add("encoder", encoder);

            // The next module in the layer is Spatial Pooler. This module will receive the output of the
            // encoder.
            cortexLayer.HtmModules.Add("sp", sp);

            double[] inputs = inputValues.ToArray();

            // Will hold the SDR of every inputs.
            Dictionary<double, int[]> prevActiveCols = new Dictionary<double, int[]>();

            // Will hold the similarity of SDKk and SDRk-1 fro every input.
            Dictionary<double, double> prevSimilarity = new Dictionary<double, double>();

            //
            // Initiaize start similarity to zero.
            foreach (var input in inputs)
            {
                prevSimilarity.Add(input, 0.0);
                prevActiveCols.Add(input, new int[0]);
            }

            // Learning process will take 1000 iterations (cycles)
            int maxSPLearningCycles = 1000;
            int numStableCycles = 0;

            for (int cycle = 0; cycle < maxSPLearningCycles; cycle++)
            {
                Debug.WriteLine($"Cycle  ** {cycle} ** Stability: {isInStableState}");

                //
                // This trains the layer on input pattern.
                foreach (var input in inputs)
                {
                    double similarity;

                    // Learn the input pattern.
                    // Output lyrOut is the output of the last module in the layer.
                    // 
                    var lyrOut = cortexLayer.Compute((object)input, true) as int[];

                    // This is a general way to get the SpatialPooler result from the layer.
                    var activeColumns = cortexLayer.GetResult("sp") as int[];

                    var actCols = activeColumns.OrderBy(c => c).ToArray();

                    similarity = MathHelpers.CalcArraySimilarity(activeColumns, prevActiveCols[input]);

                    Debug.WriteLine($"[cycle={cycle.ToString("D4")}, i={input}, cols=:{actCols.Length} s={similarity}] SDR: {Helpers.StringifyVector(actCols)}");

                    prevActiveCols[input] = activeColumns;
                    prevSimilarity[input] = similarity;
                }

                if (isInStableState)
                {
                    numStableCycles++;
                }

                if (numStableCycles > 5)
                    break;
            }

            return sp;
        }
        /// <summary>
        /// Runs an experiment to encode input values, process them using a spatial pooler, and analyze similarity.
        /// </summary>
        /// <param name="sp">The spatial pooler used for processing.</param>
        /// <param name="encoder">The encoder used for encoding input values.</param>
        /// <param name="inputValues">The list of input values to be processed.</param>
        private void RunRustructuringExperiment(SpatialPooler sp, EncoderBase encoder, List<double> inputValues)
        {
            // Create a directory to save the bitmap output.
            string outFolder = nameof(RunRustructuringExperiment);

            // Check if the directory exists, delete it if it does, then recreate it.
            if (Directory.Exists(outFolder))
            {
                Directory.Delete(outFolder, true);
            }
            Directory.CreateDirectory(outFolder);

            // Iterate through each input value.
            foreach (var input in inputValues)
            {
                // Encode the input value.
                var inpSdr = encoder.Encode(input);

                // Convert the encoded input into a 2D array.
                int[,] twoDimenArray = ArrayUtils.Make2DArray<int>(inpSdr, (int)Math.Sqrt(inpSdr.Length), (int)Math.Sqrt(inpSdr.Length));
                var twoDimArray = ArrayUtils.Transpose(twoDimenArray);

                // Draw a bitmap image of the encoded input and save it.
                NeoCortexUtils.DrawBitmap(twoDimArray, 1024, 1024, $"{outFolder}\\{input}.png", Color.Gray, Color.Green, text: $"Input image of integer {input} before reconstruction");

                // Compute active columns using the spatial pooler.
                var actCols = sp.Compute(inpSdr, false);

                // Reconstruct probabilities from the active columns.
                var probabilities = sp.Reconstruct(actCols);
                Debug.WriteLine(probabilities.Values);


                Dictionary<int, double> normalizedData = new Dictionary<int, double>();
                normalizedData = Normalize(probabilities);


                // Collecting the permancences value and applying threshold and analyzing it

                // Extract the values from the dictionary.
                Dictionary<int, double>.ValueCollection values = normalizedData.Values;

                // Initialize an array to store thresholded values.
                int[] thresholdValues = new int[inpSdr.Length];

               

                // Set the threshold value.
                var thresholds = 0.6;
                thresholdValues = ApplyThreshold(normalizedData.Values, thresholds,inpSdr.Length);

                // Calculate similarity between the original encoded input and the thresholded values.
                int matchingCount = inpSdr.Zip(thresholdValues, (a, b) => a.Equals(b) ? 1 : 0).Sum();
                var similarity = (double)matchingCount / inpSdr.Length * 100;
                similarity = Math.Round(similarity, 2);
                Debug.WriteLine($"Similarity of integer {input}: {similarity}%");

                // Convert similarity to string for file naming.
                var similaritystrng = similarity.ToString();

                // Convert thresholded values to a 2D array.
                int[,] twoDiArray = ArrayUtils.Make2DArray<int>(thresholdValues, (int)Math.Sqrt(thresholdValues.Length), (int)Math.Sqrt(thresholdValues.Length));
                var twoDArray = ArrayUtils.Transpose(twoDiArray);

                // Draw a bitmap image of the thresholded values, including similarity percentage, and save it.
                NeoCortexUtils.DrawBitmap(twoDArray, 1024, 1024, $"{outFolder}\\{input}-similarity={similaritystrng}.png", Color.Gray, Color.Green, text: $"Reconstructed Image of integer {input} with Similarity = {similaritystrng}%");
            }
        }
        /// <summary>
        /// Runs an experiment for restructuring an image
        /// </summary>
        /// <param name="sp1">The spatial pooler used for restructuring the image.</param>
        private static void RunRustructuringExperimentImage(SpatialPooler sp1)
        {
            //Create a directory to save the bitmap output.
            string outFolder = nameof(RunRustructuringExperiment);
            Directory.Delete(outFolder, true);
            Directory.CreateDirectory(outFolder);

            var inpSdr = BinarImage();  // Binarize the image and convert it to an input SDR

            // Invert the binary values to match the spatial pooler's expected input
            int[] inpSdr1 = inpSdr.Select(x => x == 1 ? 0 : 1).ToArray();
            int[,] twoDimenArray = ArrayUtils.Make2DArray<int>(inpSdr1, (int)Math.Sqrt(inpSdr1.Length), (int)Math.Sqrt(inpSdr1.Length));
            var twoDimArray = ArrayUtils.Transpose(twoDimenArray);

            // Draw the input bitmap before reconstruction
            NeoCortexUtils.DrawBitmap(twoDimArray, 1024, 1024, $"{outFolder}\\input.png", Color.Gray, Color.Green, text: $"Binarized input image before reconstruction");
            var actCols = sp1.Compute(inpSdr1, false);

            var probabilities = sp1.Reconstruct(actCols);
            Dictionary<int, double> normalizedData = new Dictionary<int, double>();
            normalizedData = Normalize(probabilities);


            //Collecting the permancences value and applying threshold and analyzing it
            Dictionary<int, double>.ValueCollection values = normalizedData.Values;
            int[] thresholdValues = new int[inpSdr1.Length];


            var thresholds = 0.3;     // Just declared the variable for segrigating values between 0 and 1 and to change the threshold value
            thresholdValues = ApplyThreshold(normalizedData.Values, thresholds,inpSdr1.Length);


            int matchingCount = inpSdr1.Zip(thresholdValues, (a, b) => a.Equals(b) ? 1 : 0).Sum();
            var similarity = (double)matchingCount / inpSdr.Length * 100;
            similarity = Math.Round(similarity, 2);
            Console.WriteLine($"Similarity: {similarity}%");

            // Draw the reconstructed output image with similarity percentage
            var similaritystrng = similarity.ToString();
            int[,] twoDiArray = ArrayUtils.Make2DArray<int>(thresholdValues, (int)Math.Sqrt(thresholdValues.Length), (int)Math.Sqrt(thresholdValues.Length));
            var twoDArray = ArrayUtils.Transpose(twoDiArray);
            NeoCortexUtils.DrawBitmap(twoDArray, 1024, 1024, $"{outFolder}\\Output-{similaritystrng}.png", Color.Gray, Color.Green, text: $"Reconstructed Image with Similarity = {similaritystrng}%");

        }

        public static Dictionary<int, double> Normalize(Dictionary<int, double> probabilities)
        {
            Dictionary<int, double> normalizedData = new Dictionary<int, double>();

            // Check if there are elements in probabilities
            if (probabilities.Count > 0)
            {
                // Find min and max values
                double minValue = double.MaxValue;
                double maxValue = double.MinValue;
                foreach (var value in probabilities.Values)
                {
                    if (value < minValue)
                        minValue = value;
                    if (value > maxValue)
                        maxValue = value;
                }

                // Normalize data
                foreach (var kvp in probabilities)
                {
                    double normalizedValue = (kvp.Value - minValue) / (maxValue - minValue);
                    normalizedData.Add(kvp.Key, normalizedValue);
                }
            }
            else
            {
                // Handle case where probabilities dictionary is empty
                Console.WriteLine("Warning: No elements in the 'probabilities' dictionary.");
            }

            return normalizedData;
        }

        /// <summary>
        /// Binarizes an image and converts the resulting binary data into an array of integers.
        /// </summary>
        /// <returns>An array of integers representing the binary data of the image.</returns>
        private static int[] BinarImage()
        {
            NeoCortexUtils.BinarizeImage("D:\\Code-X\\neocortexapi\\Black_square.JPG", "D:\\Code-X\\neocortexapi\\abcs.txt", 130, "");
            string file = "D:\\Code-X\\neocortexapi\\abcs.txt"; //..++ for image binarizer

            //NeoCortexUtils.BinarizeImage("E:\\SE\\Main projeh\\neocortexapi\\Black_square.JPG", "E:\\SE\\Main projeh\\abcs.txt", 130, "");
            //string file = "E:\\SE\\Main projeh\\abcs.txt"; //..++ for image binarizer

            string n = "";

            StreamReader r = new StreamReader(file);
            n = r.ReadToEnd();
            Console.WriteLine(n);
            int[] binaryArray = new int[n.Length];
            for (int i = 0; i < n.Length; i++)
            {
                if (int.TryParse(n[i].ToString(), out int digit))
                {
                    binaryArray[i] = digit;
                }
                else
                {
                    // Handle parsing failure, if needed
                    // For example, you might set a default value or log an error
                    binaryArray[i] = 1; // Set a default value
                                        // Log error, e.g., Console.WriteLine($"Error parsing character at position {i}");
                }
            }
            return binaryArray;
        }
        public static int[] ApplyThreshold(IEnumerable<double> values, double threshold,int length)
        {
            int[] thresholdValues = new int[length];
            int index = 0;

            foreach (var val in values)
            {
                if (val > threshold)
                {
                    thresholdValues[index] = 1;
                }
                else
                {
                    thresholdValues[index] = 0;
                }
                index++;
            }

            return thresholdValues;
        }

       
    }
}
