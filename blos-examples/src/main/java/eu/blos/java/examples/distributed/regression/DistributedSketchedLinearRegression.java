package eu.blos.java.examples.distributed.regression;


import eu.blos.java.sketches.*;
import eu.blos.scala.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;

class InputSpaceDetails implements Serializable {
	public InputSpaceDetails(){
	}
	public double[] min;
	public double[] max;
	public int resolution;
}

public class DistributedSketchedLinearRegression {

	// this value have to be set propperly
	public static int learner_parallism = 2;
	public static final String FIELD_DELIMITER = ",";
	public static final int resolution = 2;
	public static int regression_iterations = 20;
	public static final String NAME = "Distributed Sketched Linear Regression";
	private static final Log LOG = LogFactory.getLog(DistributedSketchedLinearRegression.class);

	public static String getSketchedPath(String path){
		return path+"/sketched";
	}
	public static String getStatisticsPath(String path){
		return path+"/statistics";
	}
	public static String getResultsPath(String path){
		return path+"/results";
	}

	/**
	 * find the execution environment
	 * @param cmd
	 * @return
	 */
	public static ExecutionEnvironment getEnv(CommandLine cmd){
		final ExecutionEnvironment env;
		if(cmd.hasOption("remote")){
			//"localhost", 6123, "/home/kay/blos/blos.jar"
			String[] remote = cmd.getOptionValue("remote").split(":");
			env = ExecutionEnvironment.createRemoteEnvironment(remote[0], Integer.parseInt(remote[1]), remote[2] );
		} else {
			env = ExecutionEnvironment.getExecutionEnvironment();
		}
			return env;
	}

	/**
	 * Main class
	 * @param args
	 */
	public static void main(String[] args ) throws Exception {
		LOG.info("starting");

		HelpFormatter lvFormater = new HelpFormatter();
		CommandLine cmd = parseArguments(args);

		if (cmd.hasOption('h') || (cmd.getArgs().length == 0 && cmd.getOptions().length == 0) ) {
			lvFormater.printHelp( NAME, lvOptions );
			return;
		}

		int mb = 1024*1024;
		Runtime runtime = Runtime.getRuntime();

		LOG.info("max memory space "+runtime.maxMemory() / mb);

		final ExecutionEnvironment env = getEnv(cmd);

		String inputPath				= cmd.getOptionValue("input-path");
		String outputPath 				= cmd.getOptionValue("output-path");
		String[] inputSketch2_param 	= cmd.getOptionValue("sketch").split(":");

		Double delta = Double.parseDouble(inputSketch2_param[0]);
		Double epsilon = Double.parseDouble(inputSketch2_param[1]);

		//--sketch2 0.005:0.000005
		CMSketch sketch_samples = new CMSketch( delta, epsilon );

		InputSpaceDetails inputspaceinfo = new InputSpaceDetails();

		inputspaceinfo.min				= new double[]{ Double.parseDouble( cmd.getOptionValue("min").split(":")[0] ) ,
														Double.parseDouble( cmd.getOptionValue("min").split(":")[1]) };

		inputspaceinfo.max				= new double[]{ Double.parseDouble( cmd.getOptionValue("max").split(":")[0]) ,
														Double.parseDouble( cmd.getOptionValue("max").split(":")[1]) };

		inputspaceinfo.resolution		= Integer.parseInt( cmd.getOptionValue("resolution") );

		learner_parallism				= Integer.parseInt(cmd.getOptionValue("worker"));
		regression_iterations			= Integer.parseInt(cmd.getOptionValue("iterations"));


		LOG.info("input-path: " + inputPath);
		LOG.info("preprocessor-path: "+outputPath+"/preprocessed");
		LOG.info("sketcher-path: " + outputPath+"/sketched");

		LOG.info("sketch samples w="+sketch_samples.w() );
		LOG.info("sketch samples d="+sketch_samples.d() );

		LOG.info("sketch samples size in mb:"+ (sketch_samples.alloc_size())/1024.0/1024.0 );


		if (cmd.hasOption('d') ) {
			LOG.info("run in describe-mode");
			return;
		}

		// build sketches which are distributed across the nodes
		Sketch[] sketches = { sketch_samples };

		// ------------------------------------------
		// sketcher phase
		// ------------------------------------------
		if(cmd.hasOption("sketcher")){
			LOG.info("starting sketcher");
			sketch(env, inputPath, getSketchedPath(outputPath), sketch_samples, inputspaceinfo.resolution);
		}

		// ------------------------------------------
		// learner phase
		// ------------------------------------------
		if(cmd.hasOption("learner")){
			LOG.info("starting learner");

			new LinearRegressionSketchLearner().execute(env,
					getSketchedPath(outputPath),
					getResultsPath(outputPath),
					delta, epsilon,
					learner_parallism,
					inputspaceinfo,
					regression_iterations);
		}

	}

	public static Double discretize(Double value, int precesison){
		Double scale = Math.pow(10, precesison);
		return Math.round(value * scale) / scale;
	}


	/**
	 * sketch the dataset. this output can be used for the
	 **/
	public static void sketch(final ExecutionEnvironment env,
							  String inputPath,
							  String sketchPath,
							  CMSketch sketch,
							  int res ) throws Exception {

		// hash function paramters
		final Long w = sketch.w();
		final Long d = sketch.d();
		final Integer resolution = res;

		DataSet<Tuple3<Long, Long, Long>> sketched =
			env	.readTextFile(inputPath)
				.mapPartition(new MapPartitionFunction<String, Tuple3<Long,Long,Long>>() {
					@Override
					public void mapPartition(Iterable<String> samples,
											 Collector<Tuple3<Long,Long,Long>> out)
							throws Exception {

						HashFunction[] hashfunctions =
								DigestHashFunction.generateHashfunctions((int) d.intValue(), w );
						Iterator<String> it = samples.iterator();
						while(it.hasNext()) {
							String line = it.next();
							String[] values = line.split(",");
							Double[] datapoint =
									{ discretize(Double.parseDouble(values[1]), resolution),
									discretize(Double.parseDouble(values[2]), resolution) };
							String key = "("+StringUtils.join(datapoint,FIELD_DELIMITER)+")";
							for( int d = 0; d < hashfunctions.length; d++  ){
								Long _w = hashfunctions[d].hash(key);
								Long _d = (long)d;
								Long _count = 1L;

								out.collect(new Tuple3<>( _w, _d, _count));
							}//for
						}//while
					}
				})
				.groupBy(0, 1)  // group by (w,d)
				.reduce(new ReduceFunction<Tuple3<Long, Long, Long>>() {
					@Override
					public Tuple3<Long, Long, Long>
					reduce(Tuple3<Long, Long, Long> left,
						   Tuple3<Long, Long, Long> right)
							throws Exception {
						return
								new Tuple3<Long, Long, Long>(
										left.f0,
										left.f1,
										left.f2 + right.f2);
					}
				});

		sketched.writeAsCsv(
				sketchPath,
				"\n",
				FIELD_DELIMITER,
				FileSystem.WriteMode.OVERWRITE);
		env.execute();
	}


	/**
	 * parse the input parameters
	 * @param args
	 * @return
	 */
	public static Options lvOptions = new Options();
	public static CommandLine parseArguments(String[] args ) throws Exception {
		lvOptions.addOption("h", "help", false, "shows valid arguments and options");
		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("input-path")
						.withDescription("dataset input path. necessary for preporcessing")
								//.isRequired()
						.withValueSeparator('=')
						.hasArg()
						.create("i")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("output-path")
						.withDescription("learning output path")
						//.isRequired()
						.withValueSeparator('=')
						.hasArg()
						.create("o"));

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketcher")
						.withDescription("enables sketcher. this is only necessary for the first run.")
								//.isRequired()
								//.withValueSeparator('=')
								//s.hasArg()
						.create("s")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("min")
						.withDescription("inputspace min x:y")
								//.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("m")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("max")
						.withDescription("inputspace max x:y")
								//.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("M")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("worker")
						.withDescription("number of sketch worker")
								//.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("W")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("resolution")
						.withDescription("inputspace resolution")
								//.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("R")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("iterations")
						.withDescription("iterations for gradient decent")
								//.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("n")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("learner")
						.withDescription("enables learner. this is only necessary for the first run.")
								//.isRequired()
								//.withValueSeparator('=')
								//.hasArg()
						.create("l"));

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("remote")
						.withDescription("remote cluster-setup [HOST:PORT:JAR-FILE]")
								//.isRequired()
								.withValueSeparator('=')
								.hasArg()
						.create("r"));


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch")
						.withDescription("set parameters for sketch1 delta:epsilon")
								.isRequired()
								//.withValueSeparator('=')
								.hasArg()
						.create("S"));


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("describe")
						.withDescription("runs the application in the describe-mode. This does not execute any distributed algorithms")
								//.isRequired()
								//.withValueSeparator('=')
								//.hasArg()
						.create("d"));



		withArguments(lvOptions);
		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;
		cmd = lvParser.parse(lvOptions, args);
		return cmd;
	}

	public static void withArguments(Options options){}

}
