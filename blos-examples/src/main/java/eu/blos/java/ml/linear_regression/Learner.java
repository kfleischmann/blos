package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.Sketch;
import eu.blos.java.api.common.LearningFunction;
import eu.blos.java.flink.helper.DataSetStatistics;
import eu.blos.java.flink.sketch.api.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class Learner {

	public static DataSetStatistics statistics;

	private static final Log LOG = LogFactory.getLog(Learner.class);

	/**
	 * Run the Learning phase
	 * @param env
	 * @param preprocessedPath
	 * @param sketchedPath
	 * @param outputPath
	 * @param sketches
	 * @param args
	 * @throws Exception
	 */
	public static void learn(final ExecutionEnvironment env,
							 String preprocessedPath,
							 String sketchedPath,
							 String outputPath,
							 Sketch[] sketches,
							 String ... args ) throws Exception {

		// prepare environment, distribute the sketches to all nodes and start learning phase
		readSketchesAndLearn(env, new String[]{
						sketchedPath + "/sketch_labels",
						sketchedPath + "/sketch_samples",
				},
				outputPath,
				sketches);
	}

	/**
	 *
	 * @param env
	 * @param sketchSources
	 * @param outputPath
	 * @throws Exception
	 */
	public static void readSketchesAndLearn( final ExecutionEnvironment env,
											 String[] sketchSources,
											 String outputPath,
											 final Sketch[] sketches ) throws Exception {

		LOG.info("start reading sketches into memory ");

		// read sketches into memory
		DataSet<Tuple2<String,String>> SketchDataSet = null;

		for( String source : sketchSources ) {
			if(SketchDataSet == null ){
				SketchDataSet = env.readTextFile( source ).map(new MapSketchType(new Path(source).getName()));
			} else {
				// read new source
				DataSet<Tuple2<String,String>> sketchDataSet = env.readTextFile( source ).map(
											new MapSketchType(new Path(source).getName()));

				// append reading
				SketchDataSet = SketchDataSet.union( sketchDataSet  );
			}
		}

		// do the learning
		// TODO: remove setParallelism. Flink does not support on full stream
		DataSet<Tuple1<String>> trees = SketchDataSet.mapPartition(
					new LineareRegressionLearningOperator(sketches)).setParallelism(1);

		// emit result
		if(eu.blos.java.ml.random_forest.Builder.fileOutput) {
			trees.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
		} else {
			trees.print();
		}

		// execute program
		env.execute("Learning phase");
	}


	/**
	 * add the sketch-type-name to each tuple. this is an important step to allow the sketch-reading put the tuples
	 * into the right sketch
	 */
	static class MapSketchType implements Serializable, MapFunction<String, Tuple2<String, String>> {
		private String sketchType;

		public MapSketchType(String sketchType){
			this.sketchType = sketchType;
		}

		@Override
		public Tuple2<String, String> map(String s) throws Exception {
			return new Tuple2<String, String>(this.sketchType, s);
		}
	}



	/**
	 * LinearRegression LearningOperator
	 *
	 * this operator reads the sketch into memory built in the previous phase and starts the learning process
	 * output: final trees
	 */
	static class LineareRegressionLearningOperator implements Serializable, LearningFunction<Tuple1<String>>,
						MapPartitionFunction<Tuple2<String, String>, Tuple1<String>> {
		private static final Log LOG = LogFactory.getLog(LineareRegressionLearningOperator.class);

		// --------------------------------------------------
		// SKETCH STRUCTURE for the learning phase
		// --------------------------------------------------


		private CMSketch sketch_labels;
		private CMSketch sketch_samples;

		private Collector<Tuple1<String>> output;


		public LineareRegressionLearningOperator(Sketch[] sketches) {
			super();

			sketch_labels = (CMSketch) sketches[0];
			sketch_samples = (CMSketch) sketches[1];

		}

		// ----------------------------------------------------
		// LOAD SKETCH INTO MEMORY
		// ----------------------------------------------------

		/**
		 * read the sketch, put it into a sketch structure
		 *
		 * @param sketch
		 * @param output
		 * @throws Exception
		 */
		@Override
		public void mapPartition(Iterable<Tuple2<String, String>> sketch, Collector<Tuple1<String>> output)
					throws Exception {
			this.output = output;

			//sketch_qj.allocate();
			sketch_labels.alloc();
			sketch_samples.alloc();

			Iterator<Tuple2<String, String>> it = sketch.iterator();
			while (it.hasNext()) {
				Tuple2<String, String> sketchData = it.next();

				String sketchType = sketchData.f0;
				String sketchFields = sketchData.f1;

				if(sketchType.compareTo("sketch_labels") == 0 ) {
					String[] fields = sketchFields.split(",");
					Long w = Long.parseLong(fields[0]);
					Long d = Long.parseLong(fields[1]);
					Float count = Float.parseFloat(fields[3]);

					sketch_labels.array_set(d,w,count);
				}

				if(sketchType.compareTo("sketch_samples") == 0 ) {
					String[] fields = sketchFields.split(",");
					Long w = Long.parseLong(fields[0]);
					Long d = Long.parseLong(fields[1]);
					Float count = Float.parseFloat(fields[3]);

					sketch_samples.array_set(d,w,count);
				}
			}

			LOG.info("finished reading sketches into memory");


			learn(output);
		}

		public static List<Tuple2<Double,Double[]>> testLinRegDataSet(){
			List<Tuple2<Double,Double[]>> dataset = new ArrayList<Tuple2<Double,Double[]>>();
			dataset.add(new Tuple2(-0.955629435186,  new Double[] {1.0, -0.75121113185}) );
			dataset.add(new Tuple2(0.490889720885,  new Double[] {1.0, 0.585311356523})) ;
			dataset.add(new Tuple2(-1.07238545278,  new Double[] {1.0, -0.925939426578}));
			dataset.add(new Tuple2(-0.390171914177,  new Double[] {1.0, -0.272969938626}));
			dataset.add(new Tuple2(0.782689711998,  new Double[] {1.0, 0.828812491524} ));
			dataset.add(new Tuple2(0.637338224205,  new Double[] {1.0, 0.78592062834} ));
			dataset.add(new Tuple2(-0.227083652156,  new Double[] {1.0, -0.0966025660222} ));
			dataset.add(new Tuple2(0.309557927922,  new Double[] {1.0, 0.4713667385} ));
			dataset.add(new Tuple2(-0.38246690061,  new Double[] {1.0, -0.229493696328} ));
			dataset.add(new Tuple2(-0.399638414267, new Double[] { 1.0, -0.194375304678} ));
			return dataset;
		}

		@Override
		public void learn(Collector<Tuple1<String>> output) {
			LOG.info("start learning phase");

			// m*x+b
			Double alpha=0.05;
			Double[] theta = {0.0, 0.0};
			Double[] theta_old ={0.0, 0.0};

			for( int i=0; i < 100; i++ ) {
				LOG.info("learned model (iteration "+i+"): "+theta[0]+" "+theta[1]);

				theta[0] = theta_old[0] - alpha*nextStep(0, theta_old);
				theta[1] = theta_old[1] - alpha*nextStep(1, theta_old);

				theta_old[0] = theta[0];
				theta_old[1] = theta[1];
			}
		}

		private List<Tuple2<Double,Double[] >> dataset = testLinRegDataSet();
		private Double nextStep( int k, Double[] theta ){
			int d = theta.length;
			Double sum=0.0;
			for( int i=0; i < statistics.getSampleCount(); i++ ) {
				//result+= - dataset.get(i).f0*dataset.get(i).f1[k];
				sum+= - sketch_labels.get( SketchBuilder.constructKey(i,k) );

			}//for

			for( int j=0; j < d; j++ ) {
				for (int i = 0; i < statistics.getSampleCount(); i++) {
					//sum += theta[j] * dataset.get(i).f1[j] * dataset.get(i).f1[k];
					sum += theta[j] * sketch_samples.get(SketchBuilder.constructKey(i,j,k) );
				}//for
			}//for

			return sum / (double) statistics.getSampleCount();
		}

	}
}
