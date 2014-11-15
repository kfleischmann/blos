package eu.blos.java.ml.linear_regression;


import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.java.algorithms.sketches.Sketch;
import eu.blos.java.flink.helper.DataSetStatistics;
import eu.blos.java.flink.helper.StatisticsBuilder;
import eu.blos.java.flink.helper.SampleFormat;
import eu.blos.java.flink.sketch.api.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Builder {
	private static final Log LOG = LogFactory.getLog(Builder.class);

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
		env.setDegreeOfParallelism(1);

		return env;
	}

	/**
	 *
	 * @param args
	 */
	public static void main(String[] args ) throws Exception {
		CommandLine cmd = parseArguments(args);
		final ExecutionEnvironment env = getEnv(cmd);

		String rawInputPath	= 			cmd.getOptionValue("input-path");
		String preprocessedDataPath=  	cmd.getOptionValue("preprocessing-path");
		String sketchDataPath=  		cmd.getOptionValue("sketch-path");
		String outputPath = 			cmd.getOptionValue("output-path");

		CMSketch sketch_labels = new CMSketch(0.1 /*factor*/, 0.0001 /*prob*/);
		CMSketch sketch_samples = new CMSketch(0.1 /*factor*/, 0.0001 /*prob*/);



		StatisticsBuilder.run(env, rawInputPath, outputPath + "/statistics", new SampleFormat(",", " ", -1, 2));
		Learner.statistics = StatisticsBuilder.read(env, outputPath + "/statistics");

		System.out.println(rawInputPath);
		System.out.println(preprocessedDataPath);
		System.out.println(sketchDataPath);

		System.out.println(sketch_samples.w() );
		System.out.println(sketch_samples.d() );
		System.out.println("size in mb:"+ (sketch_samples.w()*sketch_samples.d())*4.0/1024.0/1024.0 );

		// ------------------------------------------ls
		// start preprocessing phase
		// ------------------------------------------
		Preprocessor.transform(env, rawInputPath, preprocessedDataPath);


		// -----------------------------------------
		// start sketching phase
		// ------------------------------------------
		SketchBuilder.sketch(env,
				preprocessedDataPath, sketchDataPath,
				SketchBuilder.apply(
						"sketch_labels",/*input preprocessed*/ "sketch_labels",  /* output sketch */
						sketch_labels.get_hashfunctions().toArray(new HashFunction[sketch_labels.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
								2,	// emit y-value
								SketchBuilder.Fields(0,1)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				),
				SketchBuilder.apply( 	"sketch_samples", /*input*/ "sketch_samples",  /*output*/
						sketch_samples.get_hashfunctions().toArray(new HashFunction[sketch_samples.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
								3,	// emit y-value
								SketchBuilder.Fields(0,1,2)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				)
		);


		Sketch[] sketches = {sketch_labels, sketch_samples };

		Learner.learn(env, preprocessedDataPath, sketchDataPath, outputPath+"/results", sketches, "1");

	}
	/**
	 * parse the input parameters
	 * @param args
	 * @return
	 */
	public static CommandLine parseArguments(String[] args ) throws Exception {
		Options lvOptions = new Options();

		lvOptions.addOption("h", "help", false, "shows valid arguments and options");

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("input-path")
						.withDescription("raw input-data path. necessary for preporcessing")
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
						.withLongOpt("preprocessing-path")
						.withDescription("preprocessor results path")
						//.isRequired()
						.withValueSeparator('=')
						.hasArg()
						.create("P"));

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch-path")
						.withDescription("sketcher results path")
						//.isRequired()
						.withValueSeparator('=')
						.hasArg()
						.create("S"));

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("learn-path")
						.withDescription("Learner result path")
						//.isRequired()
						.withValueSeparator('=')
						.hasArg()
						.create("L"));

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



		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("execute")
						.withDescription("describes the execution steps. p process, s setch,l learn. E.g. -E=psl")
						.withValueSeparator('=')
						.hasArg()
						.create("E"));


		withArguments(lvOptions);

		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;

		cmd = lvParser.parse(lvOptions, args);

		return cmd;
	}

	public static void withArguments(Options options){}
}
