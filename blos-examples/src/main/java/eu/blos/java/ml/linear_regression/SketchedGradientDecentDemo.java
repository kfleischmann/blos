package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.RoundNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.ZeroOneNormalizer;
import eu.blos.java.flink.sketch.api.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SketchedGradientDecentDemo {
	//public static List<Tuple2<Double,Double>> dataset = new ArrayList<Tuple2<Double,Double>>();

	public static List<Tuple1<Double>> labels = new ArrayList<Tuple1<Double>>();
	public static List<CMSketch> sketch1 = new ArrayList<CMSketch>();
	public static List<CMSketch> sketch2 = new ArrayList<CMSketch>();
	public static long datasetSize = 0;
	public static int numIterations = 0;

	public static FieldNormalizer normalizer;

	public static CommandLine cmd;
	public static void main(String[] args) throws Exception {
		HelpFormatter lvFormater = new HelpFormatter();
		cmd = parseArguments(args);

		if (cmd.hasOption('h') || (cmd.getArgs().length == 0 && cmd.getOptions().length == 0) ) {
			lvFormater.printHelp( "Sketched Regression", lvOptions );
			return;
		}

		// make it possible to read from stdin
		InputStreamReader is=null;
		if( cmd.getOptionValue("input").equals("stdin") ){
			is = new InputStreamReader(System.in);
		} else {
			is = new FileReader(new File(cmd.getOptionValue("input")));
		}

		// prepare normalizer from input
		normalizer =  new RoundNormalizer( Integer.parseInt(cmd.getOptionValue("normalization-space")) );

		if( cmd.hasOption("verbose")) System.out.println(cmd.getOptionValue("input"));

		String[] inputSketch1_param		= cmd.getOptionValue("sketch1").split(":");
		String[] inputSketch2_param 	= cmd.getOptionValue("sketch2").split(":");
		String[] inputSketch3_param 	= cmd.getOptionValue("sketch3").split(":");
		String[] inputSketch4_param 	= cmd.getOptionValue("sketch4").split(":");
		String[] inputSketch5_param 	= cmd.getOptionValue("sketch5").split(":");
		String[] inputSketch6_param 	= cmd.getOptionValue("sketch6").split(":");

		numIterations = Integer.parseInt( cmd.getOptionValue("iterations") );

		double total_size=0.0;

		sketch1.add( new CMSketch( Double.parseDouble(inputSketch1_param[0]), Double.parseDouble(inputSketch1_param[1]) ) );
		sketch1.add( new CMSketch( Double.parseDouble(inputSketch2_param[0]), Double.parseDouble(inputSketch2_param[1]) ) );
		sketch2.add( new CMSketch( Double.parseDouble(inputSketch3_param[0]), Double.parseDouble(inputSketch3_param[1]) ) );
		sketch2.add( new CMSketch( Double.parseDouble(inputSketch4_param[0]), Double.parseDouble(inputSketch4_param[1]) ) );
		sketch2.add( new CMSketch( Double.parseDouble(inputSketch5_param[0]), Double.parseDouble(inputSketch5_param[1]) ) );
		sketch2.add( new CMSketch( Double.parseDouble(inputSketch6_param[0]), Double.parseDouble(inputSketch6_param[1]) ) );


		for( CMSketch s : sketch1 ){
			s.alloc();
			if( cmd.hasOption("verbose")) System.out.println("Sketch-size: w="+s.w()+", d="+s.d());

			total_size += s.alloc_size();
		}

		for( CMSketch s : sketch2 ){
			s.alloc();
			if( cmd.hasOption("verbose"))  System.out.println("Sketch-size: w="+s.w()+", d="+s.d());
			total_size += s.alloc_size();
		}

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
					double yi_xik0 = (double) Yi.getField(0) * (double) Xi.getField(k);
					lookup = ""+normalizer.normalize(yi_xik0);

					sketch1.get(k).update(lookup);
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


	// real model: -0.14164507397556597 1.0434789479935427
	public static List<Tuple2<Double,Double[]>> testLinRegDataSet(){
		List<Tuple2<Double,Double[]>> dataset = new ArrayList<Tuple2<Double,Double[]>>();
		dataset.add(new Tuple2(-0.955629435186,  new Double[] {1.0, -0.75121113185}) );
		dataset.add(new Tuple2(0.490889720885,  new Double[] {1.0, 0.585311356523})) ;
		dataset.add(new Tuple2(-1.07238545278,  new Double[] {1.0, -0.925939426578}));
		dataset.add(new Tuple2(-0.390171914177,  new Double[] {1.0, -0.272969938626}));
		dataset.add(new Tuple2(0.782689711998,  new Double[] {1.0, 0.828812491524} ));
		dataset.add(new Tuple2(0.637338224205,  new Double[] {1.0, 0.78592062834} ));
		dataset.add(new Tuple2(-0.227083652156,  new Double[] {1.0, -0.0966025660222} ));
		dataset.add(new Tuple2(0.309557927922,  new Double[] {1.0, 0.4713667385} ));
		dataset.add(new Tuple2(-0.38246690061,  new Double[] {1.0, -0.229493696328} ));
		dataset.add(new Tuple2(-0.399638414267, new Double[] {1.0, -0.194375304678} ));
		return dataset;
	}

	/**
	 * learn the model
	 */
	public static void learn() {
		Double alpha=0.5;
		Double[] theta = {0.9, 0.9};
		Double[] theta_old = {0.9, 0.9};

		for( int i=0; i < numIterations; i++ ) {
			theta[0] = theta_old[0] - alpha*nextStepSkeched(0, theta_old);
			theta[1] = theta_old[1] - alpha*nextStepSkeched(1, theta_old);

			theta_old[0] = theta[0];
			theta_old[1] = theta[1];

			if( cmd.hasOption("verbose") || cmd.hasOption("all-results")) System.out.println( theta[0] + " "+theta[1]);
		}//for
		System.out.println( "final-model: "+theta[0] + " "+theta[1]);
	}

	/**
	 * computes one step for the following iteration
	 *
	 * @param k
	 * @param theta
	 * @return
	 */
	public static Double nextStepSkeched( int k, Double[] theta ){
		int d = theta.length;
		//Double result=0.0;
		Double sum = 0.0;
		// real gradient decent
		//for( int i=0; i < dataset.size(); i++ ) {
			//result+= - labels.get(i).f0 * (double)dataset.get(i).getField(k) / (double)dataset.size();
		//}//for
		sum += -sketchEstimate( sketch1.get(k) , normalizer )  / (double)datasetSize;

		for( int j=0; j < d; j++ ) {
			// real gradient decent
			//for (int i = 0; i < dataset.size(); i++) {
				//result += theta[j] * (double)dataset.get(i).getField(j) * (double)dataset.get(i).getField(k) / (double)dataset.size();
			//}//for

			sum += theta[j] * sketchEstimate( sketch2.get(k*2+j) , normalizer ) / (double)datasetSize;
		}//for
		return sum;
	}

	/**
	 * that function estimate a sum over a specific dataset
	 * @param sketch sketch to read from the estimate
	 * @param normalizer normalizer to reconstruct the "real" value if encoded. Can be potential the identity function
	 * @return
	 */
	public static Double sketchEstimate(CMSketch sketch, FieldNormalizer normalizer ){
		double sum = 0.0;
		long counter = sketch.totalSumPerHash();
		long freq;
		String lookup;
		for(double l=(double)normalizer.getMin(); l < (double)normalizer.getMax(); l+=(double)normalizer.getStep() ){
			lookup = ""+normalizer.normalize(l);
			freq =  sketch.get(""+lookup);
			sum += l * freq;
			counter += freq;
		}//for
		return sum;
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
						.withLongOpt("sketch1")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s1")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch2")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s2")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch3")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s3")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch4")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s4")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch5")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s5")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("sketch6")
						.withDescription("sketch size")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("s6")
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
						.withLongOpt("display-sketches")
						.withDescription("display the sketch content")
								//.withValueSeparator('=')
								//.hasArg()
						.create("d")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("normalization-space")
						.withDescription("normalization-space")
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
