package eu.blos.java.ml.random_forest;

import eu.blos.scala.algorithms.Histogram;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.*;
import eu.stratosphere.api.java.operators.*;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;
import java.util.Iterator;


public class RFSketching {

	public static String PATH_OUTPUT_SPLIT_CANDIDATES = "feature_split_candidates";
	public static String PATH_OUTPUT_SKETCH = "rf_sketch";

	// context data
	public static boolean fileOutput =  true;
	public static int numFeatures = 784;
	public static int maxBins = 10;
	public static int maxSplitCandidates = 5;


	// these values must be estimated, during the sketching phase
	public static int NUMBER_LABELS  = 10;
	public static int NUMBER_FEATURES = 784;
	public static int NUMBER_SAMPLES = 10000;


    public static void run(final ExecutionEnvironment env, String inputPath, String outputPath ) throws Exception {
		new Path(outputPath).getFileSystem().delete(new Path(outputPath), true );
		new Path(outputPath).getFileSystem().mkdirs(new Path(outputPath));


		String outputCandidates = outputPath+"/"+PATH_OUTPUT_SPLIT_CANDIDATES;
		String outputSketch = outputPath+"/"+PATH_OUTPUT_SKETCH;

		computeSplitCandidates(inputPath, outputCandidates,  env, maxSplitCandidates);

		buildSketches(inputPath, outputSketch, env, outputCandidates);
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
	public static void computeSplitCandidates(String inputPath, String outputPath, ExecutionEnvironment env,
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

							// TODO: setup feature count

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
		DataSet filtered_splitCandidates =
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
			filtered_splitCandidates.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE );
		} else {
			filtered_splitCandidates.print();
		}

		// execute program
		env.execute("Sketching phase");

	}

	/**
	 * build sketch data. This data is used for the learning phase
	 *
	 * Output (sketchId, sketchKey, value)
	 *
	 * @param inputPath
	 * @param outputPath
	 * @param env
	 * @throws Exception
	 */
	public  static void buildSketches( String inputPath, String outputPath, ExecutionEnvironment env,
									   String outputCandidates ) throws Exception  {
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
		// output: sketch_type,sampleId,label,featureId,featureValue,SplitCandidate
		DataSet<Tuple6<String,Integer,Integer,Integer,Double, Double>> cout =  sampleFeatures
			.joinWithTiny(candidates)
			.where(2)
			.equalTo(0)
			.with(new JoinFunction< Tuple4<Integer,Integer,Integer,Double>, Tuple2<Integer, Double>, Tuple6<String, Integer,Integer,Integer,Double, Double>>(){
			@Override
			public Tuple6<String, Integer, Integer, Integer, Double, Double> join(Tuple4<Integer, Integer, Integer, Double> sampleFeature, Tuple2<Integer, Double> candidate) throws Exception {
				return new Tuple6<String, Integer, Integer, Integer, Double, Double>( "node-sketch", sampleFeature.f0, sampleFeature.f1, sampleFeature.f2, sampleFeature.f3, candidate.f1 );
			}
		});



		// emit result
		if(fileOutput) {
			cout.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE );
		} else {
			cout.print();
		}

		// execute program
		env.execute("Sketching phase");

	}
}
