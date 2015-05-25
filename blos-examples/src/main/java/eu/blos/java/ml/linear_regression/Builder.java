package eu.blos.java.ml.linear_regression;


import eu.blos.java.algorithms.sketches.Sketch;
import eu.blos.java.flink.helper.StatisticsBuilder;
import eu.blos.java.flink.helper.SampleFormat;
import eu.blos.java.flink.sketch.api.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Builder {
	public static final String NAME = "Linear Regression Builder";
	private static final Log LOG = LogFactory.getLog(Builder.class);

	public static String getPreprocessedPath(String path){
		return path+"/preprocessed";
	}
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

		// TODO: replace that.
		env.setDegreeOfParallelism(1);

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

		final ExecutionEnvironment env = getEnv(cmd);

		String inputPath		= 			cmd.getOptionValue("input-path");
		String outputPath 		= 			cmd.getOptionValue("output-path");

		CMSketch sketch_labels = new CMSketch(0.5 /*factor*/, 0.000001 /*prob*/);
		CMSketch sketch_samples = new CMSketch(0.5 /*factor*/, 0.000001 /*prob*/);

		LOG.info("input-path: "+inputPath);
		LOG.info("preprocessor-path: "+outputPath+"/preprocessed");
		LOG.info("sketcher-path: " + outputPath+"/sketched");

		LOG.info("w="+sketch_samples.w() );
		LOG.info("d="+sketch_samples.d() );
		LOG.info("size in mb:"+ (sketch_samples.w()*sketch_samples.d())*4.0/1024.0/1024.0 );


		StatisticsBuilder.run(env, inputPath, getStatisticsPath(outputPath), new SampleFormat(",", " ", -1, 2));
		Learner.statistics = StatisticsBuilder.read(env, getStatisticsPath(outputPath) );


		// build sketches which are distributed
		Sketch[] sketches = {sketch_labels, sketch_samples };


		// ------------------------------------------
		// preprocessing phase
		// ------------------------------------------
		if(cmd.hasOption("preprocessor")){
			LOG.info("starting preprocessor");
			preprocess( env, inputPath, getPreprocessedPath( outputPath) );
		}

		// ------------------------------------------
		// sketcher phase
		// ------------------------------------------
		if(cmd.hasOption("sketcher")){
			LOG.info("starting sketcher");
			sketch(env, getPreprocessedPath(outputPath), getSketchedPath(outputPath), sketches);
		}

		// ------------------------------------------
		// learner phase
		// ------------------------------------------
		if(cmd.hasOption("learner")){
			LOG.info("starting learner");
			learn( env, outputPath, sketches );
		}
	}

	/**
	 * learn from preprocessed data and write model into output path
	 * @param env apache flink environement
	 * @param path where to find the data?
	 * @param sketches list of sketches to be build
	 * @throws Exception
	 */
	public static void learn( final ExecutionEnvironment env, String path, Sketch[] sketches ) throws Exception {
		Learner.learn(env,
				getPreprocessedPath(path),
				getSketchedPath(path),
				getResultsPath(path), sketches, "1");
	}

	/**
	 * preapre the raw input dataset for the sketching phase
	 *
	 * input-format
	 * 		index,label,x-values (separated by space delimiter)
	 *
	 *
	 * OUTPUT:
	 * 	LABELS:
	 *
	 * 	output-format
	 *		(x,y) -> (i,k ) => (i,k,xk^i * y^i )
	 *
	 * 	SAMPLES:
	 *
	 * 	output-format
	 *		(x,y) -> (i, k, j) =>  (i,k,j,xk^i * xj^i)
	 *
	 *		i: sample-index
	 *		k: feature-index
	 *		j: feature-index
	 *		x: sample
	 *		y: label
	 *
	 * @param env
	 * @param input
	 * @param preprocessPath
	 */
	public static void preprocess(final ExecutionEnvironment env, String input, String preprocessPath ) throws Exception {
		Preprocessor.transform(env, input, preprocessPath);
	}

	/**
	 * sketch the preprocessed data. this output can be used for the
	 * learning phase.
	 * @param env
	 * @param preprocessPath
	 * @param sketchPath
	 * @param sketches containing details about how tho sketch the output
	 * @throws Exception
	 */
	public static void sketch(final ExecutionEnvironment env,
							  String preprocessPath,
							  String sketchPath,
							  Sketch[] sketches ) throws Exception {
		SketchBuilder.sketch(env,
				preprocessPath, sketchPath,
				SketchBuilder.apply(
						"sketch_labels",/*input preprocessed*/
						"sketch_labels",  /* output sketch */
						sketches[0].getHashfunctions(),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								SketchBuilder.FIELD_DELIMITER, // split line by comma
								2,	// emit y-value
								SketchBuilder.Fields(0,1)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				)
				,
				SketchBuilder.apply(
						"sketch_samples",
						"sketch_samples",
						sketches[1].getHashfunctions(),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								SketchBuilder.FIELD_DELIMITER, // split line by comma
								3,	// emit y-value
								SketchBuilder.Fields(0,1,2)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				)
		);
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
						.create("i"));

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
				.withLongOpt("preprocessor")
				.withDescription("enables preprocessor. this is only necessary for the first run.")
					//.isRequired()
					//.withValueSeparator('=')
					//.hasArg()
				.create("p"));

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketcher")
						.withDescription("enables sketcher. this is only necessary for the first run.")
								//.isRequired()
								//.withValueSeparator('=')
								//.hasArg()
						.create("s"));

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

		withArguments(lvOptions);
		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;
		cmd = lvParser.parse(lvOptions, args);
		return cmd;
	}

	public static void withArguments(Options options){}

}
