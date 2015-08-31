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

	public static FieldNormalizer normalizer;

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

	}


	public static void buildSketches() {
		// prepare normalizer from input
		normalizer =  new RoundNormalizer( Integer.parseInt(cmd.getOptionValue("normalization-space")) );


		if( cmd.hasOption("verbose")) System.out.println(cmd.getOptionValue("input"));

		String[] inputSketch1_param		= cmd.getOptionValue("sketch1").split(":");
		numIterations = Integer.parseInt( cmd.getOptionValue("iterations") );

		double total_size=0.0;

		sketch = new CMSketch( Double.parseDouble(inputSketch1_param[0]), Double.parseDouble(inputSketch1_param[1]) );

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
				Tuple2<Double,Double> Xi = new Tuple2<>( 1.0, Double.parseDouble(values[2]) );

				for(int k=0; k < 2; k++ ) {
					double yi_xik0 = (double) Yi.getField(0) * (dou

					ble) Xi.getField(k);
					lookup = ""+normalizer.normalize(yi_xik0);

					sketch.get(k).update(lookup);

					for (int j = 0; j < 2; j++) {
						double xij_xik0 = (double) Xi.getField(j) * (double) Xi.getField(k);

						max = Math.max( max, xij_xik0);
						min = Math.min( min, xij_xik0);

						lookup=""+normalizer.normalize(xij_xik0);
						sketch2.get(k*2+j).update(lookup);
					}//for
				}//for


				lines++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}


		if( cmd.hasOption("display-sketches")) {
			for( CMSketch s : sketch1 ){
				s.display();
				System.out.println();
			}//for
		}//if

		if( cmd.hasOption("display-sketches")) {
			for (CMSketch s : sketch2) {
				s.display();
				System.out.println();
			}//for
		}//if

		learn();
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



		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;
		cmd = lvParser.parse(lvOptions, args);
		return cmd;
	}
}
