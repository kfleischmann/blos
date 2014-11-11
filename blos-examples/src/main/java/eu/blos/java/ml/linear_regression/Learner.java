package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.algorithms.sketches.Sketch;
import eu.blos.java.api.common.LearningFunction;
import eu.blos.java.ml.random_forest.RFBuilder;
import eu.blos.java.ml.random_forest.RFPreprocessing;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.*;

public class Learner {
	private static final Log LOG = LogFactory.getLog(Learner.class);

	/**
	 * Run the Learning phase
	 *
	 * @param env
	 * @param sketchDataPath
	 * @param outputTreePath
	 * @param sketches
	 * @param args
	 * @throws Exception
	 */
	public static void learn(final ExecutionEnvironment env,
							 String preprocessedDataPath,
							 String sketchDataPath,
							 String outputTreePath,
							 Sketch[] sketches,
							 String ... args ) throws Exception {

		// prepare environment, distribute the sketches to all nodes and start learning phase
		readSketchesAndLearn(env, new String[]{
						sketchDataPath + "/sketch_labels",
						sketchDataPath + "/sketch_samples",
				},
				outputTreePath,
				sketches);

	}

	/**
	 *
	 * @param env
	 * @param sketchSources
	 * @param outputPath
	 * @throws Exception
	 */
	public static void readSketchesAndLearn( final ExecutionEnvironment env, String[] sketchSources, String outputPath, final Sketch[] sketches ) throws Exception {

		LOG.info("start reading sketches into memory ");

		// read sketches into memory
		DataSet<Tuple2<String,String>> SketchDataSet = null;

		for( String source : sketchSources ) {
			if(SketchDataSet == null ){
				SketchDataSet = env.readTextFile( source ).map(new MapSketchType(new Path(source).getName()));
			} else {
				// read new source
				DataSet<Tuple2<String,String>> sketchDataSet = env.readTextFile( source ).map(new MapSketchType(new Path(source).getName()));

				// append reading
				SketchDataSet = SketchDataSet.union( sketchDataSet  );
			}
		}

		// do the learning
		DataSet<Tuple1<String>> trees = SketchDataSet.mapPartition(new LineareRegressionLearningOperator(sketches)).setParallelism(1);

		// emit result
		if(RFBuilder.fileOutput) {
			trees.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
		} else {
			trees.print();
		}

		// execute program
		env.execute("Learning phase");
	}


	/**
	 *
	 */
	static class MapSketchType extends MapFunction<String, Tuple2<String,String>> implements Serializable {
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
	 * LinearRegression LearningOprator
	 *
	 * this operator reads the sketch into memory built in the previous phase and starts the learning process
	 * output: final trees
	 */
	static class LineareRegressionLearningOperator extends MapPartitionFunction<Tuple2<String,String>, Tuple1<String>> implements Serializable, LearningFunction<Tuple1<String>> {
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
		public void mapPartition(Iterator<Tuple2<String, String>> sketch, Collector<Tuple1<String>> output) throws Exception {
			this.output = output;

			//sketch_qj.allocate();
			sketch_labels.alloc();
			sketch_samples.alloc();

			while (sketch.hasNext()) {
				Tuple2<String, String> sketchData = sketch.next();

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

			// ---------------------------------------
			// START LEARNING PHASE
			// ---------------------------------------


			learn(output);
		}

		@Override
		public void learn(Collector<Tuple1<String>> output) {
			System.out.println("learn");


		}
	}
}
