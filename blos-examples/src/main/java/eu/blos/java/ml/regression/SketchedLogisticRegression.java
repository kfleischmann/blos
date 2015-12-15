package eu.blos.java.ml.regression;

import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.fieldnormalizer.RoundNormalizer;
import eu.blos.scala.algorithms.sketches.CMEstimate;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SketchedLogisticRegression {


	public static CMSketch sketch = new CMSketch();
	public static long datasetSize = 0;
	public static int numIterations = 0;
	public static double[] consts = new double[2];
	public static int numHeavyHitters = 100;

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

		String absolutePath = new File(cmd.getOptionValue("input")).getAbsolutePath();
		File file = new File( absolutePath.substring(0,absolutePath.lastIndexOf(File.separator)) +"/heavy_hitters" );
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("index,y,x");
		bw.newLine();

		for( int k=1; k < sketch.getHeavyHitters().getHeapArray().length; k++ ){
			CMEstimate topK = (CMEstimate)sketch.getHeavyHitters().getHeapArray()[k];
			if(topK!=null) {
				String[] values = topK.key().replaceAll("[^-0-9,.E]","").split(",");
				// y, x
				Tuple2<Double,Double> d = new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]) ) ;
				System.out.println("val ("+k+"): "+topK.count()+" => " + d);

				bw.write(k+","+d.f0+","+d.f1 );
				bw.newLine();

			}
		}
		bw.close();



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

		sketch = new CMSketch( Double.parseDouble(inputSketchSize_param[0]), Double.parseDouble(inputSketchSize_param[1]), numHeavyHitters );

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
				if( cmd.hasOption("verbose"))  if(lines%1000 == 0) System.out.println("read lines "+lines);

				//Tuple1<Double> Yi = new Tuple1<Double>( Double.parseDouble(values[1]) );
				//Tuple2<Double,Double> Xi = new Tuple2<>(  normalizer.normalize(1.0), normalizer.normalize(Double.parseDouble(values[2])) );
				//Tuple1<Double> Xi = new Tuple1<>(normalizer.normalize(Double.parseDouble(values[2])));

				//dataset.add( Xi );
				//labels.add(Yi);
				//lookup = Xi.toString() ;
				//sketch.update(lookup);

				// (y,x)
				Tuple2<Double,Double> Xi = new Tuple2<>(  normalizer.normalize(Double.parseDouble(values[1])), normalizer.normalize(Double.parseDouble(values[2])) );

				lookup = Xi.toString() ;
				sketch.update(lookup);


				// for each dimension
				//for(int k=0; k < 2; k++ ) {
				//	consts[k] += Yi.f0* (double)Xi.getField(k);
				//}//for


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
		Double alpha=1.0;

		Tuple2<Double,Double> theta = new Tuple2<Double,Double>(2.0, 4.0);
		Tuple2<Double,Double> theta_old = new Tuple2<Double,Double>(2.0, 4.0);

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
		return sketchGradientDecentUpdateEstimateWithHeavyHitters(theta, k) ;
		//return sketchGradientDecentUpdateEstimate( theta, k ) / (double)datasetSize;
		//return realGradientDecentUpdateEstimate( theta, k ) / (double)datasetSize;
	}


	/**
	 *
	 * @param k
	 * @param Xi
	 * @param model
	 * @return
	 */
	public static double G_k_theta(int k, Tuple2<Double,Double> Xi, Tuple2<Double,Double>  model){
		return g_theta(Xi, model ) * (double)Xi.getField(k);
	}

	public static double g_theta(  Tuple2<Double,Double> Xi,Tuple2<Double,Double>  model){
		return (1.0 / (1.0+ Math.exp( -(Xi.f0*model.f0 + Xi.f1*model.f1 ))));
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

			sum +=  G_k_theta( k, new Tuple2<>(1.0, l), model )* freq;
		}//for

		return sum - consts[k];
	}

	public static Double sketchGradientDecentUpdateEstimateWithHeavyHitters(Tuple2<Double,Double> model, int k  ){
		double sum = 0.0;
		double sumy = 0.0;
		long total_freq = 0;
		long freq;

		for( int s=1; s < sketch.getHeavyHitters().getHeapArray().length; s++ ) {
			CMEstimate topK = (CMEstimate)sketch.getHeavyHitters().getHeapArray()[s];
			if(topK!=null) {
				String[] values = topK.key().replaceAll("[^-0-9,.E]","").split(",");

				// (y,x)
				Tuple1<Double> Yi = new Tuple1<>(Double.parseDouble(values[0]));
				Tuple2<Double, Double> Xi = new Tuple2<>( 1.0, Double.parseDouble(values[1]));

				//System.out.println("learn on: "+Yi+", "+Xi);
				freq = topK.count();
				total_freq += 1;
				sum +=  G_k_theta( k, Xi, model );

				sumy += (Yi.f0 * (double)Xi.getField(k));
			}
		}

		return (sum - sumy) / total_freq;
	}

	public static Double realGradientDecentUpdateEstimate(Tuple2<Double,Double> model, int k  ) {
		Double sum=0.0;
		for( int i=0; i < datasetSize; i++ ){
			sum += (g_theta( dataset.get(i), model ) - labels.get(i).f0 ) * (double)dataset.get(i).getField(k);
		}
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
