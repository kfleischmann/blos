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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;


public class RFPreprocessing {

	private static final Log LOG = LogFactory.getLog(RFPreprocessing.class);

	public static final String PATH_OUTPUT_SKETCH_SPLIT_CANDIDATES = "feature-split-candidates";
	public static final String PATH_OUTPUT_SKETCH_NODE = "rf-sketch";
	public static final String PATH_OUTPUT_SKETCH_SAMPLE_LABELS = "sample-labels";


	public static int HISTOGRAM_MAX_BINS 			= 5;
	public static int HISTOGRAM_SPLIT_CANDIDATES 	= 5;

	// TODO: these values should be estimated, during the preprocessing phase

	public static int NUM_SAMPLE_LABELS  			= 10;
	public static int NUM_SAMPLE_FEATURES 			= 784;
	public static int NUM_SAMPLES 					= 60000;


	/**
	 * do the preprocessing for the sketching phase. reads the raw data from hdfs and prepare the data for the
	 * sketching phase
	 *
	 * @param env
	 * @param inputPath
	 * @param outputPath
	 * @param args
	 * @throws Exception
	 */
    public static void transform(final ExecutionEnvironment env, String inputPath, String outputPath, String ... args ) throws Exception {

		LOG.info("start preprocessing phase");

		// prepare
		new Path(outputPath).getFileSystem().delete( new Path(outputPath), true );
		new Path(outputPath).getFileSystem().mkdirs( new Path(outputPath) );

		String outputBaggingTable = outputPath+"/"+PATH_OUTPUT_SKETCH_SAMPLE_LABELS;
		String outputCandidates = outputPath+"/"+PATH_OUTPUT_SKETCH_SPLIT_CANDIDATES;
		String outputSketch = outputPath+"/"+PATH_OUTPUT_SKETCH_NODE;

		// compute split candidates
		computeSplitCandidates(inputPath, outputCandidates,  env, HISTOGRAM_SPLIT_CANDIDATES);

		// build the raw input data that is read from the sketcher
		buildSketchRawData(env, inputPath, outputBaggingTable, outputCandidates, outputSketch);
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

		LOG.info("compute split candidates with maxSplitCandidates="+maxSplitCandidates);

		ReduceOperator<Tuple2<Integer, String>> histograms =

				samples.mapPartition(new MapPartitionFunction<String, Tuple2<Integer, String>>() {
					@Override
					public void mapPartition(Iterator<String> samples, Collector<Tuple2<Integer, String>> histogramCollector) throws Exception {
						Histogram[] histograms = new Histogram[NUM_SAMPLE_FEATURES];

						for (int i = 0; i < NUM_SAMPLE_FEATURES; i++) {
							histograms[i] = new Histogram(i, HISTOGRAM_MAX_BINS);
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

						for (int i = 0; i < NUM_SAMPLE_FEATURES; i++) {
							histogramCollector.collect(new Tuple2<Integer, String>(i, histograms[i].toString()));
						}//for
					}
				})
				.groupBy(0) // group by featureNr
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

		// construct output
		MapOperator<Tuple2<Integer, String>, Tuple2<Integer, String>> outputFormat =
				filtered_splitCandidates.map( new MapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(Tuple2<Integer, String> tuple ) throws Exception {
						return new Tuple2<Integer,String>(tuple.f0, tuple.f1 );
					}
				});

		// emit result
		if(RFBuilder.fileOutput) {
			outputFormat.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE );
		} else {
			outputFormat.print();
		}

		// execute program
		env.execute("Preprocessing");

	}

	/**
	 * build raw data for the sketching phase
	 *
	 *
	 * @param env
	 * @param inputPath
	 * @param outputPathBaggingTable
	 * @param outputCandidates
	 * @param outputPathSketch
	 * @throws Exception
	 */
	public  static void buildSketchRawData( ExecutionEnvironment env,
									   String inputPath,
									   String outputPathBaggingTable,
									   String outputCandidates,
									   String outputPathSketch ) throws Exception  {

		LOG.info("start building sketches for learning phase");

		// read samples
		DataSet<String> samples = env.readTextFile(inputPath);


		// output: (sampleId, label, featureId, featureValue)
		DataSet<Tuple4<Integer,Integer,Integer,Double>> sampleFeatures =
					samples.flatMap( new FlatMapFunction<String, Tuple4<Integer,Integer,Integer,Double> >() {
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

		// output: split-candidate,<featureId>,<split-candidate-val> (featureId, splitCandidate)
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
		// output: sampleId,label,featureId,featureValue,SplitCandidate
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




		// construct output
		MapOperator<String, Tuple2<String, String>> sampleLabels = samples.map( new MapFunction<String, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(String sample) throws Exception {
				String[] values = sample.split(" ");
				String lineId = values[0];
				String label = values[1];
				return new Tuple2<String, String>( lineId, label );
			}
		});


		// emit result
		if(RFBuilder.fileOutput) {
			cout.writeAsCsv(outputPathSketch, "\n", ",", FileSystem.WriteMode.OVERWRITE );
			sampleLabels.writeAsCsv(outputPathBaggingTable, "\n", ",", FileSystem.WriteMode.OVERWRITE );
		} else {
			cout.print();
			sampleLabels.print();
		}

		// execute program
		env.execute("Preprocessing");

	}
}
