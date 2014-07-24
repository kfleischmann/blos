package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.scala.algorithms.Histogram;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.*;
import eu.stratosphere.api.java.operators.*;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;


public class RFSketching {

	public static boolean fileOutput =  true;
	public static int numFeatures = 784;
	public static int maxBins = 10;
	public static int maxSplitCandidates = 5;
	public static int numLabels = 10;


    public static void main(String[] args) throws Exception {
		//final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String inputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String outputPath=  "file:///home/kay/temp/rf/"; //sketch_splitcandidates_mnist_normalized_small";



		String outputCandidates = outputPath+"/feature_split_candidates";
		String outputSketch = outputPath+"/samples_sketch";

		// -------------------------------
		// Skeching-Phase
		// -------------------------------

		double epsilon = 0.00000000001;
		double delta = 0.001;

		computeSplitCandidates(inputPath, outputCandidates,  env, maxSplitCandidates);

		buildSketches(inputPath, outputPath + "/samples_sketch", env, epsilon, delta, outputCandidates);

		// -------------------------------
		// Learning-Phase
		// -------------------------------

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
	public  static void computeSplitCandidates(String inputPath, String outputPath, ExecutionEnvironment env,
											   final int maxSplitCandidates ) throws Exception  {
		DataSet<String> samples = env.readTextFile(inputPath);

		ReduceOperator<Tuple2<Integer, String>> histograms =

				samples.mapPartition(new MapPartitionFunction<String, Tuple2<Integer, String>>() {
					@Override
					public void mapPartition(Iterator<String> samples, Collector<Tuple2<Integer, String>> histogramCollector) throws Exception {
						Histogram[] histograms = new Histogram[numFeatures];

						for (int i = 0; i < numFeatures; i++) {
							histograms[i] = new Histogram(i, maxBins);
						}//for

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
						}//for
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
						return new Tuple2<Integer,String>( histogram.f0, Histogram.fromString( histogram.f1 )
														.uniform(maxSplitCandidates).mkString(" ") );
					}
				});

		// filter invalid features (without split-candidates)
		DataSet output =
		splitCandidates.filter( new FilterFunction<Tuple2<Integer, String>>() {
			@Override
			public boolean filter(Tuple2<Integer, String> splitCand) throws Exception {
				// f0 is the feature id
				// f1 list of valid split-candidates
				if( splitCand.f1.trim().length() > 0 )
					return true;
				else
					return false;
			}
		});

		// emit result
		if(fileOutput) {
			output.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE );
		} else {
			output.print();
		}

		// execute program
		env.execute("Sketching example");

	}

	/**
	 * build sketch data. This data is used for the learning phase
	 *
	 * Output (sketchId, sketchKey, value)
	 *
	 * @param inputPath
	 * @param outputPath
	 * @param env
	 * @param epsilon
	 * @param delta
	 * @throws Exception
	 */
	public  static void buildSketches( String inputPath, String outputPath, ExecutionEnvironment env,
									 double epsilon, double delta, String outputCandidates ) throws Exception  {

		int d = (int)Math.ceil(Math.log(1 / delta));
		long w = (long)Math.ceil(Math.exp(1) /epsilon);

		final HashFunction[] hashfunctions = HashFunction.generateHashfunctions(d, w );

		// read samples
		DataSet<String> samples = env.readTextFile(inputPath);


		// output: (sampleId, label, featureId, featureValue)
		DataSet<Tuple4<Integer,Integer,Integer,Double>> sampleFeatures = samples.flatMap( new FlatMapFunction<String, Tuple4<Integer,Integer,Integer,Double> >() {
			@Override
			public void flatMap(String sample, Collector<Tuple4<Integer, Integer, Integer, Double>> collector) throws Exception {

				String[] values = sample.split(" ");
				Integer lineId = Integer.parseInt( values[0] );
				Integer label = Integer.parseInt( values[1] );
				Integer featureId = 0;

				int numFeatures = values.length - 2;
				for (int i = 2; i < values.length; i++) {
					featureId = i-2;
					collector.collect( new Tuple4<Integer, Integer, Integer, Double>( lineId, label, featureId, Double.parseDouble(values[i]) ));
				}//for
			}
		});

		// output: featureId, split-candidate (featureId, splitCandidate)
		FlatMapOperator<String, Tuple2<Integer, Double>> candidates = env.readTextFile(outputCandidates)
			.flatMap(new FlatMapFunction<String, Tuple2<Integer, Double>>() {
				@Override
				public void flatMap(String s, Collector<Tuple2<Integer, Double>> collector) throws Exception {
					String[] values = s.split(",");
					String[] candidates = values[1].split(" ");
					for( String cand : candidates ){
						collector.collect( new Tuple2<Integer, Double>(Integer.parseInt(values[0]) /*feature*/, Double.parseDouble(cand) /*split canidates*/));
					}//for
				}
			});


		// join by featureId
		DataSet<Tuple5<Integer,Integer,Integer,Double, Double>> cout =  sampleFeatures
			.joinWithTiny(candidates)
			.where(2)
			.equalTo(0)
			.with(new JoinFunction< Tuple4<Integer,Integer,Integer,Double>, Tuple2<Integer, Double>, Tuple5<Integer,Integer,Integer,Double, Double>>(){
			@Override
			public Tuple5<Integer, Integer, Integer, Double, Double> join(Tuple4<Integer, Integer, Integer, Double> sampleFeature, Tuple2<Integer, Double> candidate) throws Exception {
				return new Tuple5<Integer, Integer, Integer, Double, Double>( sampleFeature.f0, sampleFeature.f1, sampleFeature.f2, sampleFeature.f3, candidate.f1 );
			}
		});

		//DataSet<Tuple4<String, Long, Integer, Integer>> hashed = samples.flatMap( new SketchBuilder(hashfunctions) );

		// emit result
		if(fileOutput) {
			cout.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE );
		} else {
			cout.print();
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
