package eu.blos.java.ml.gradient_decent;

import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.RoundNormalizer;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class GradientDecent {


	public static CMSketch sketch = new CMSketch();
	public static long datasetSize = 0;
	public static int numIterations = 0;
	public static double[] consts = new double[2];

	public static FieldNormalizer<Double> normalizer;

	public static CommandLine cmd;

	public static void main(String[] args) throws Exception {
		HelpFormatter lvFormater = new HelpFormatter();
		cmd = parseArguments(args);

		if (cmd.hasOption('h') || (cmd.getArgs().length == 0 && cmd.getOptions().length == 0)) {
			lvFormater.printHelp("Sketched Regression", lvOptions);
			return;
		}

		// make it possible to read from stdin
		InputStreamReader is = null;
		if (cmd.getOptionValue("input").equals("stdin")) {
			is = new InputStreamReader(System.in);
		} else {
			is = new FileReader(new File(cmd.getOptionValue("input")));
		}


		buildSketches(is);

		sketch.display();
	}


	/**
	 * learn the model
	 */
	public static void learn() {
		Double alpha=0.5;

		Tuple2<Double,Double> theta = new Tuple2<Double,Double>(0.9, 0.9);
		Tuple2<Double,Double> theta_old = new Tuple2<Double,Double>(0.9, 0.9);

		for( int i=0; i < numIterations; i++ ) {
			theta.f0 = theta_old.f0 - alpha*nextStepSkeched(0, theta_old);
			theta.f1 = theta_old.f1 - alpha*nextStepSkeched(1, theta_old);

			theta_old.f0 = theta.f0;
			theta_old.f1 = theta.f1;

			if( cmd.hasOption("verbose") || cmd.hasOption("all-results")) System.out.println( theta.f0 + " "+theta.f1);
		}//for
		System.out.println( "final-model: "+theta.f0 + " "+theta.f1);
	}

	/**
	 * computes one step for the following iteration
	 *
	 * @param k
	 * @param theta
	 * @return
	 */
	public static Double nextStepSkeched( int k, Tuple2<Double,Double> theta ){
		return sketchGradientDecentUpdateEstimate( theta, k );
	}


	/**
	 *
	 * @param is
	 */
	public static void buildSketches( InputStreamReader is ) {

		// prepare normalizer from input
		normalizer =  new RoundNormalizer( Integer.parseInt(cmd.getOptionValue("normalization-space")) );


		if( cmd.hasOption("verbose")) System.out.println(cmd.getOptionValue("input"));

		String[] inputSketchSize_param		= cmd.getOptionValue("sketch").split(":");
		numIterations = Integer.parseInt( cmd.getOptionValue("iterations") );

		double total_size=0.0;

		sketch = new CMSketch( Double.parseDouble(inputSketchSize_param[0]), Double.parseDouble(inputSketchSize_param[1]) );

		sketch.alloc();
		if( cmd.hasOption("verbose")) System.out.println("Sketch-size: w="+sketch.w()+", d="+sketch.d());
		if( cmd.hasOption("verbose")) System.out.println("total sketch-size: "+ (total_size/1024.0/1024.0 )+"mb");

		double max=0.0;
		double min=0.0;

		long lines=0;
		try (BufferedReader br = new BufferedReader( is )) {
			String line;

			String lookup;

			while ((line = br.readLine()) != null) {
				String[] values =line.split(",");
				datasetSize++;

				// some debug messages
				if( cmd.hasOption("verbose"))  if(lines%100000 == 0) System.out.println("read lines "+lines);

				Tuple1<Double> Yi = new Tuple1<Double>( Double.parseDouble(values[1]) );
				Tuple2<Double,Double> Xi = new Tuple2<>(  normalizer.normalize(1.0), normalizer.normalize(Double.parseDouble(values[2])) );
				//Tuple1<Double> Xi = new Tuple1<>(normalizer.normalize(Double.parseDouble(values[2])));


				lookup = Xi.toString() ;

				sketch.update(lookup);

				// for each dimension
				for(int k=0; k < 2; k++ ) {
					consts[k] += Yi.f0* (double)Xi.getField(k);
				}//for


				lines++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *
	 * @param k
	 * @param Xi
	 * @param model
	 * @return
	 */
	public static double G_k_theta(int k, Tuple2<Double,Double> Xi, Tuple2<Double,Double>  model){
		return (1.0 / (1.0+ Math.exp( Xi.f0*model.f0 + Xi.f1*model.f1 ))) * (double)Xi.getField(k);
	}

	/**
	 *
	 * @param model
	 * @param k
	 * @return
	 */
	public static Double sketchGradientDecentUpdateEstimate(Tuple2<Double,Double> model, int k  ){
		double sum = 0.0;
		long freq;
		String lookup;

		// iterate through the whole input-space
		for (double l = (double) normalizer.getMin(); l < (double) normalizer.getMax(); l += (double) normalizer.getStep()) {
			lookup = "(1.0," + normalizer.normalize(l)+")";
			freq = sketch.get(lookup);

			sum +=  freq * G_k_theta( k, new Tuple2<>(1.0, l), model )* freq;
		}//for
		return - consts[k] + sum;
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
						.withLongOpt("input")
						.withDescription("set the input dataset to process")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("i")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("iterations")
						.withDescription("number of iterations")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("n")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("all-results")
						.withDescription("show all model results")
								//.withValueSeparator('=')
								//.hasArg()
						.create("a")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("verbose")
						.withDescription("verbose")
								//.withValueSeparator('=')
								//.hasArg()
						.create("v")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("normalization-space")
						.withDescription("normalization-space")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s")
		);


		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;
		cmd = lvParser.parse(lvOptions, args);
		return cmd;
	}
}
