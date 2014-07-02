package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.scala.algorithms.Histogram;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.MapOperator;
import eu.stratosphere.api.java.operators.ReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;


public class RFSketching {

	public static boolean fileOutput =  false;
	public static int numFeatures = 784;
	public static int maxBins = 10;
	public static int maxSplitCandidates = 5;

    public static void main(String[] args) throws Exception {
		//final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String inputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String outputPath=  "file:///home/kay/temp/rf"; //sketch_splitcandidates_mnist_normalized_small";

		// Skeching-Phase

		computeSplitCandidates(inputPath, outputPath+"/feature_split_candidates",  maxSplitCandidates, env);



		// Learning-Phase
	}



	/**
	 * compute split candidates by building histograms for each feature and reduce the histogram
	 * bin-size to maxSplitCandidates.
	 *
	 * @param inputPath
	 * @param outputPath
	 * @param env
	 * @throws Exception
	 */
	public  static void computeSplitCandidates(String inputPath, String outputPath, final int maxSplitCandidates, ExecutionEnvironment env ) throws Exception  {
		DataSet<String> samples = env.readTextFile(inputPath);

		ReduceOperator<Tuple2<Integer, String>> histograms =

				samples.mapPartition(new MapPartitionFunction<String, Tuple2<Integer, String>>() {
					@Override
					public void mapPartition(Iterator<String> samples, Collector<Tuple2<Integer, String>> histogramCollector) throws Exception {
						Histogram[] histograms = new Histogram[numFeatures];

						for (int i = 0; i < numFeatures; i++) {
							histograms[i] = new Histogram(i, maxBins);
						}

						while (samples.hasNext()) {
							String[] values = samples.next().split(" ");
							String lineId = values[0];
							String label = values[1];

							int numFeatures = values.length - 2;

							for (int i = 2; i < values.length; i++) {
								histograms[i - 2].update(Double.parseDouble(values[i]));
							}//for
						}//while

						for (int i = 0; i < numFeatures; i++) {
							histogramCollector.collect(new Tuple2<Integer, String>(i, histograms[i].toString()));
						}
					}
				})
				.groupBy(0)
				.reduce(new ReduceFunction<Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> reduce(Tuple2<Integer, String> t1,
														  Tuple2<Integer, String> t2) throws Exception {
						return new Tuple2<Integer, String>(t1.f0, Histogram.fromString(t1.f1).merge(Histogram.fromString(t2.f1)).toString());
					}
				});

		// compute split candidates from histograms as output
		MapOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> splitCandidates =
				histograms.map( new MapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(Tuple2<Integer, String> histogram ) throws Exception {
						return new Tuple2<Integer,String>( histogram.f0, Histogram.fromString( histogram.f1 ).uniform(maxSplitCandidates).mkString(" ") );
					}
				});

		// emit result
		if(fileOutput) {
			splitCandidates.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE );
		} else {
			splitCandidates.print();
		}

		// execute program
		env.execute("Sketching example");

	}

	public  static void buildSketch(String inputPath, String outputPath, ExecutionEnvironment env )
																									throws Exception  {
		double epsilon = 0.00000000001;
		double delta = 0.001;

		int d = (int)Math.ceil(Math.log(1 / delta));
		long w = (long)Math.ceil(Math.exp(1) /epsilon);

		System.out.println(w);
		System.out.println(d);

		final HashFunction[] hashfunctions = HashFunction.generateHashfunctions(d, w );

		// get input data
		DataSet<String> samples = env.readTextFile(inputPath);

		DataSet<Tuple4<String, Long, Integer, Integer>> hashed = samples.flatMap( new SketchBuilder(hashfunctions) );


		// emit result
		if(fileOutput) {
			hashed.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE );
		} else {
			hashed.print();
		}

		// execute program
		env.execute("Sketching example");

	}


	public static final class SketchBuilder extends FlatMapFunction<String, Tuple4<String, Long, Integer, Integer>> implements Serializable{

		public HashFunction[] hashfunctions;

		public SketchBuilder( HashFunction[] hf ){
			hashfunctions = hf;

			System.out.println("SketchBuilder");
		}


		@Override
		public void flatMap(String value, Collector<Tuple4<String, Long, Integer, Integer>> out) {
			// normalize and split the line
			String[] values = value.toLowerCase().split("\\W+");

			for(int d=0; d < hashfunctions.length; d++ ){
				HashFunction hf = hashfunctions[d];

				String key = values[0]+"-splitcandidate-"+new java.util.Random().nextInt(Integer.MAX_VALUE);

				out.collect(new Tuple4<String, Long, Integer, Integer>(key, hf.hash((long)key.hashCode()), d, 1));
			}//for
		}
	}
}
