package eu.blos.java.ml.linear_regression;


import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.stratosphere.api.java.ExecutionEnvironment;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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


		String rawInputPath	= 			cmd.getOptionValue("input-path"); //"file:///home/kay/datasets/mnist/normalized_full.txt";
		String preprocessedDataPath=  	cmd.getOptionValue("prep-path"); //"file:///home/kay/temp/rf/tree-1-test1-mnist-05/preprocessed";
		String sketchDataPath=  		cmd.getOptionValue("sketch-path"); //"file:///home/kay/temp/rf/tree-1-test1-mnist-05/sketched";
		String outputTreePath = 		cmd.getOptionValue("learn-path"); //"file:///home/kay/temp/rf/tree-1-test1-mnist-05/tree/tree";


		// allocates 5 GB memory
		CMSketch cmSketch = new CMSketch(0.01, 0.00000001);
		cmSketch.alloc();

		System.out.println(cmSketch.w() );
		System.out.println(cmSketch.d() );
		System.out.println("size in mb:"+cmSketch.size()*4/1024/1024 );

		// ------------------------------------------
		// start preprocessing phase
		// ------------------------------------------
		Preprocessor.process(env, rawInputPath, preprocessedDataPath);


		// ------------------------------------------
		// start sketching phase
		// ------------------------------------------


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
						.withLongOpt("prep-path")
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


		withArguments(lvOptions);

		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;

		cmd = lvParser.parse(lvOptions, args);

		return cmd;
	}

	public static void withArguments(Options options){}
}
