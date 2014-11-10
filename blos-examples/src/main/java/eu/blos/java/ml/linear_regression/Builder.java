package eu.blos.java.ml.linear_regression;


import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.java.ml.random_forest.RFPreprocessing;
import eu.blos.java.stratosphere.sketch.api.SketchBuilder;
import eu.blos.java.stratosphere.sketch.api.SketcherUDF;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;
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
		String preprocessedDataPath=  	cmd.getOptionValue("preprocessing-path"); //"file:///home/kay/temp/rf/tree-1-test1-mnist-05/preprocessed";
		String sketchDataPath=  		cmd.getOptionValue("sketch-path"); //"file:///home/kay/temp/rf/tree-1-test1-mnist-05/sketched";
		String outputPath = 			cmd.getOptionValue("output-path"); //"file:///home/kay/temp/rf/tree-1-test1-mnist-05/tree/tree";

		CMSketch sketch1 = new CMSketch(0.1 /*factor*/, 0.1 /*prob*/);
		CMSketch sketch2 = new CMSketch(0.1 /*factor*/, 0.01 /*prob*/);
		CMSketch sketch3 = new CMSketch(0.001 /*factor*/, 0.001 /*prob*/);
		CMSketch sketch4 = new CMSketch(0.01 /*factor*/, 0.0001 /*prob*/);
		CMSketch sketch5 = new CMSketch(0.01 /*factor*/, 0.00001 /*prob*/);

		//sketch1.alloc();
		//sketch2.alloc();

		System.out.println(rawInputPath);
		System.out.println(preprocessedDataPath);
		System.out.println(sketchDataPath);

		System.out.println(sketch1.w() );
		System.out.println(sketch1.d() );
		System.out.println("size in mb:"+ sketch1.size()*4.0/1024.0/1024.0 );

		// ------------------------------------------ls
		// start preprocessing phase
		// ------------------------------------------
		Preprocessor.transform(env, rawInputPath, preprocessedDataPath);


		// -----------------------------------------
		// start sketching phase
		// ------------------------------------------
		SketchBuilder.sketch(env,
				preprocessedDataPath, sketchDataPath,
				/*SketchBuilder.apply( 	"sketch1",	// input preprocessed
										"sketch1",  // output sketch
										sketch1.get_hashfunctions().toArray(new HashFunction[sketch1.get_hashfunctions().size()]),
										SketchBuilder.SKETCHTYPE_CM_SKETCH,
										new SketchBuilder.DefaultSketcherUDF(
												",", // split line by comma
												2,	// emit y-value
												SketchBuilder.Fields(0,1)), // extract fields for hashing (i,k)
										SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				),*/
				SketchBuilder.apply( 	"sketch2", /*input*/ "sketch2_1",  /*output*/
						sketch1.get_hashfunctions().toArray(new HashFunction[sketch1.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
								3,	// emit y-value
								SketchBuilder.Fields(0,1,2)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				),
				SketchBuilder.apply( 	"sketch2", /*input*/ "sketch2_2",  /*output*/
						sketch2.get_hashfunctions().toArray(new HashFunction[sketch2.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
								3,	// emit y-value
								SketchBuilder.Fields(0,1,2)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				),
				SketchBuilder.apply( 	"sketch2", /*input*/ "sketch2_3",  /*output*/
						sketch3.get_hashfunctions().toArray(new HashFunction[sketch3.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
								3,	// emit y-value
								SketchBuilder.Fields(0,1,2)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				),
				SketchBuilder.apply( 	"sketch2", /*input*/ "sketch2_4",  /*output*/
						sketch4.get_hashfunctions().toArray(new HashFunction[sketch4.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
								3,	// emit y-value
								SketchBuilder.Fields(0,1,2)), // extract fields for hashing (i,k)
						SketchBuilder.ReduceSketchByFields(0, 1) // group by hash
				),
				SketchBuilder.apply( 	"sketch2", /*input*/ "sketch2_5",  /*output*/
						sketch5.get_hashfunctions().toArray(new HashFunction[sketch5.get_hashfunctions().size()]),
						SketchBuilder.SKETCHTYPE_CM_SKETCH,
						new SketchBuilder.DefaultSketcherUDF(
								",", // split line by comma
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
