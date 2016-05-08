package eu.blos.java.examples.distributed.regression;

import eu.blos.scala.examples.regression.SketchedRegression;
import eu.blos.scala.inputspace.StaticInputSpace;
import eu.blos.scala.inputspace.Vectors;
import eu.blos.scala.inputspace.normalizer.Rounder;
import eu.blos.scala.sketches.CMSketch;
import eu.blos.scala.sketches.DiscoveryStrategyEnumeration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import java.io.Serializable;
import java.util.Iterator;

public class LinearRegressionSketchLearner {
	private static final Log LOG = LogFactory.getLog(LinearRegressionSketchLearner.class);

	public static void execute(final ExecutionEnvironment env,
							 String inputPath,
							 String outputPath,
							 Double delta, Double epsilon,
							 int parallelism,
							 InputSpaceDetails inputSpaceDetails,
							 int iterations ) throws Exception {

		int worker=0;

		// for each mapper create a source. This is because flink cannot stream the complete data to all nodes
		DataSet<Tuple1<String>>[] SketchDataSet = new DataSet[parallelism];
		for (DataSet<Tuple1<String>> s : SketchDataSet) {
			System.out.println("add data source");

			// write resuls of the single nodes into a folder, for each node
			s = env.readTextFile(new String(inputPath))
						.mapPartition(
								new LineareRegressionSketchOperator(iterations,
										delta, epsilon, worker, inputSpaceDetails,
										outputPath ))
								.setParallelism(1);

			s.writeAsCsv(outputPath + "/worker_models/" + worker++, "\n", ",", FileSystem.WriteMode.OVERWRITE);
		}//for
		// execute program
		env.execute("Discovery/Learning phase");

		System.out.println("start producing final model");

		// merge the results of sketch learners
		env.readTextFile(outputPath+"/worker_models/")
				.mapPartition( new MapPartitionFunction<String, Tuple1<String>>() {
			@Override
			public void mapPartition(Iterable<String> models, Collector<Tuple1<String>> out) throws Exception {

				int number_models = 0;
				Vectors.DoubleVector final_model = Vectors.EmptyDoubleVector(2);

				Iterator<String> it = models.iterator();
				while(it.hasNext()){
					String line = it.next();
					System.out.println(line);
					String[] model = line.split(",");

					final_model = final_model.$plus( new Vectors.DoubleVector(
							new double[]{Double.parseDouble(model[0]), Double.parseDouble(model[1])}) );
					number_models++;
				}

				final_model = final_model.$div( (double)number_models);
				out.collect( new Tuple1<>(""+final_model.elements()[0]+","+final_model.elements()[1]));
			}
		}).setParallelism(1).writeAsCsv(outputPath + "/final_model/", "\n", ",", FileSystem.WriteMode.OVERWRITE);

		env.execute("Merge models");
	}


	/**
	 * LinearRegression LearningOperator
	 * <p/>
	 * this operator reads the sketch into memory built in the previous phase and starts the learning process
	 * output: final trees
	 */
	static class LineareRegressionSketchOperator implements
			Serializable, MapPartitionFunction<String, Tuple1<String>> {

		private final Log LOG = LogFactory.getLog(LineareRegressionSketchOperator.class);
		private int worker;
		private Double delta;
		private Double epsilon;
		private InputSpaceDetails inputSpaceDetails;
		private String output;
		private int iterations;
		public LineareRegressionSketchOperator(int iterations, Double delta, Double epsilon, int worker,
												 InputSpaceDetails inputSpaceDetails,
												 String output) {
			super();
			this.iterations = iterations;
			this.worker = worker;
			this.epsilon = epsilon;
			this.delta = delta;
			this.inputSpaceDetails = inputSpaceDetails;
			this.output = output;
			System.out.println("init sketch worker "+worker);
		}

		// ----------------------------------------------------
		// LOAD SKETCH INTO MEMORY
		// ----------------------------------------------------

		@Override
		public void mapPartition(Iterable<String> sketch,
								 Collector<Tuple1<String>> collector)
				throws Exception {
			// allocate memory
			CMSketch cms = new CMSketch(delta, epsilon);
			cms.alloc();

			Iterator<String> it = sketch.iterator();
			while(it.hasNext()){
				String sketchFields = it.next();
				String[] fields = sketchFields.split(",");
				Long w = Long.parseLong(fields[0]);
				Long d = Long.parseLong(fields[1]);
				Long count = Long.parseLong(fields[2]);
				cms.array_set(d, w, count);
			}//while

			learn(cms, collector);
		}

		
		public void learn(CMSketch cms ,Collector<Tuple1<String>> collector) {


			Rounder discretizer = new Rounder(inputSpaceDetails.resolution);

			double[] min_stepsize = {	Math.pow(10, -inputSpaceDetails.resolution )*(worker+1)*(worker+1),
										Math.pow(10, -inputSpaceDetails.resolution )*(worker+1)*(worker+1) };

			double[] my_stepsize = {0.0+DistributedSketchedLinearRegression.learner_parallism*min_stepsize[0],
									0.0+DistributedSketchedLinearRegression.learner_parallism*min_stepsize[1]};

			double[] min = {	inputSpaceDetails.min[0]+worker*min_stepsize[0],
								inputSpaceDetails.min[1]+worker*min_stepsize[1]
							};

			double[] max = inputSpaceDetails.max;

			double alpha = 0.5;

			// learning
			// define static input space window
			StaticInputSpace inputspace = new StaticInputSpace(
					new Vectors.DoubleVector(min),
					new Vectors.DoubleVector(max),
					new Vectors.DoubleVector(my_stepsize) );
			// starting model

			Vectors.DoubleVector model = Vectors.EmptyDoubleVector(2).$plus(1);
			// sketch enumeration
			DiscoveryStrategyEnumeration discovery = new DiscoveryStrategyEnumeration(cms, inputspace, discretizer);

			// Debug reasons
			SketchedRegression.write_sketch( output+"/worker"+worker+"/", cms,
					inputspace,
					discretizer,
					new Vectors.DoubleVector(my_stepsize) );

			for( int i=0; i < iterations; i++ ) {
				model = model.$minus( SketchedRegression.gradient_decent_step(
						new SketchedRegression.LinearRegressionModel(model),
						discovery.iterator()).$times(alpha) );
			}

			// write out the final learned model
			collector.collect( new Tuple1<String>(""+model.elements()[0]+","+model.elements()[1]));
		}
	}
}
