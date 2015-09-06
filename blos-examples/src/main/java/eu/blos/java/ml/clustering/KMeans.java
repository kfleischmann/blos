package eu.blos.java.ml.clustering;


import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.RoundNormalizer;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

	public static CMSketch sketch = new CMSketch();
	public static long datasetSize = 0;
	public static int numIterations = 0;

	public static FieldNormalizer<Double> normalizer;

	public static CommandLine cmd;
	public static List<Tuple2<Double,Double>> dataset = new ArrayList<>();
	public static List<Tuple1<Double>> labels = new ArrayList<>();

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

		learn();
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
		total_size = sketch.alloc_size();

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

				Tuple2<Double,Double> Xi = new Tuple2<>(  normalizer.normalize(Double.parseDouble(values[1])), normalizer.normalize(Double.parseDouble(values[2])) );


				lookup = Xi.toString() ;

				sketch.update(lookup);

				lines++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * learn the model
	 */
	public static void learn() {

	}


	/**
	 *
	 * @return
	 */
	public static void updateClusterCentroids( Tuple2<Double,Double>[] centroids ){
		long freq;
		String lookup;

		Tuple2<Double,Double>[] sums = new Tuple2[centroids.length];

		long[] counts = new long[centroids.length] ;

		// iterate through the whole input-space
		for (double x = (double) normalizer.getMin(); x < (double) normalizer.getMax(); x += (double) normalizer.getStep()) {
			for (double y = (double) normalizer.getMin(); y < (double) normalizer.getMax(); y += (double) normalizer.getStep()) {

				Tuple2<Double,Double> value = new Tuple2<>(normalizer.normalize(x),normalizer.normalize(y));
				lookup = "("+normalizer.normalize(x)+"," + normalizer.normalize(y) + ")";
				freq = sketch.get(lookup);

				int ibestCentroid = 0;
				double currDistance = Double.MAX_VALUE;
				for (int i = 1; i < centroids.length; i++) {
					double d = Math.sqrt(
						(centroids[ibestCentroid].f0 - centroids[i].f0) * (centroids[ibestCentroid].f0 - centroids[i].f0) +
						(centroids[ibestCentroid].f0 - centroids[i].f0) * (centroids[ibestCentroid].f0 - centroids[i].f0)  );

					if( d < currDistance){
						ibestCentroid = i;
					}
				}//for

				counts[ibestCentroid] += freq;
				//sums[ibestCentroid] += value;

			}//For
		}//for
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
