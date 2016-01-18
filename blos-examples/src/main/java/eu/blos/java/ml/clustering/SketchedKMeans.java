package eu.blos.java.ml.clustering;

import eu.blos.java.algorithms.sketches.fieldnormalizer.RoundNormalizer;
import eu.blos.scala.algorithms.sketches.CMEstimate;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SketchedKMeans {
	public static CMSketch sketch = new CMSketch();
	public static long datasetSize = 0;
	public static int numIterations = 0;
	public static int numCentroids = 0;
	public static int numHeavyHitters = 100;
	public static RoundNormalizer normalizer;
	public static CommandLine cmd;
	public static List<Tuple2<Double,Double>> dataset = new ArrayList<>();
	public static List<Tuple1<Double>> labels = new ArrayList<>();
	public static Tuple2<Double,Double>[] centroids;

	public static void main(String[] args) throws Exception {
		HelpFormatter lvFormater = new HelpFormatter();

		try{
			cmd = parseArguments(args);
		} catch (Exception e){
			LOG(e.getMessage());
			lvFormater.printHelp("SketchedKMeans", lvOptions);
			return;
		}

		if ( cmd != null && (cmd.hasOption('h') || (cmd.getArgs().length == 0 && cmd.getOptions().length == 0) )) {
			lvFormater.printHelp( "SketchedKMeans", lvOptions);
			return;
		}

		numCentroids = Integer.parseInt ( cmd.getOptionValue("centroids") );
		numHeavyHitters = Integer.parseInt( cmd.getOptionValue("heavyhitters") );
		centroids = new Tuple2[numCentroids];

		// make it possible to read from stdin
		InputStreamReader is = null;
		if (cmd.getOptionValue("input").toLowerCase().equals("stdin")) {
			is = new InputStreamReader(System.in);
		} else {
			is = new FileReader(new File(cmd.getOptionValue("input")));
		}

		buildSketches(is);

		if( cmd.hasOption("verbose")){
			sketch.display(System.out);
		}

		if( cmd.hasOption("print-sketch") ) {
			// write output
			if( cmd.hasOption("output")) {
				sketch.display(System.out);
			} else {
				PrintStream out = new PrintStream(new FileOutputStream( cmd.getOptionValue("output")+"/sketch"));
				sketch.display(out);
				out.close();
			}

		}//if

		if( cmd.hasOption("verbose") || cmd.hasOption("print-sketch") ) {
			System.out.println("heavyhitters");
			for (int k = 1; k < sketch.getHeavyHitters().getHeapArray().length; k++) {
				CMEstimate topK = (CMEstimate) sketch.getHeavyHitters().getHeapArray()[k];
				if (topK != null) {
					String[] values = topK.key().replaceAll("[^-0-9,.E]","").split(",");
					Tuple2<Double, Double> d = new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]));

					System.out.println(""+k+" "+d.f0+" "+d.f1);
				}//if
			}//for
			System.out.println("");
		}//if

		learn(centroids);

		// write output
		if( cmd.hasOption("output")) {
			PrintStream out = new PrintStream(new FileOutputStream( cmd.getOptionValue("output")+"/centers"));
			for (int k = 0; k < centroids.length; k++) {
				out.println( k+" "+centroids[k].f0 + " " + centroids[k].f1);
			}//for
			out.close();

		} else {
			for (int k = 0; k < centroids.length; k++) {
				System.out.println(k+" "+ centroids[k].f0 + " " + centroids[k].f1);
			}//for
		}

	}

	/**
	 * read 2d points from input source
	 * possible
	 * - stdin
	 * - file
	 * @param is
	 */
	public static void buildSketches( InputStreamReader is ) {
		// prepare normalizer from input
		normalizer =  new RoundNormalizer( Integer.parseInt(cmd.getOptionValue("normalization-space")) );

		if( cmd.hasOption("verbose")) LOG(cmd.getOptionValue("input"));

		String[] inputSketchSize_param		= cmd.getOptionValue("sketch").split(":");
		numIterations = Integer.parseInt( cmd.getOptionValue("iterations") );

		double total_size=0.0;

		sketch = new CMSketch( Double.parseDouble(inputSketchSize_param[0]), Double.parseDouble(inputSketchSize_param[1]), numHeavyHitters );

		sketch.alloc();
		total_size = sketch.alloc_size();

		if( cmd.hasOption("verbose")) LOG("Sketch-size: w="+sketch.w()+", d="+sketch.d());
		if( cmd.hasOption("verbose")) LOG("total sketch-size: "+ (total_size/1024.0/1024.0 )+"mb");

		double max=0.0;
		double min=0.0;

		long lines=0;
		try (BufferedReader br = new BufferedReader( is )) {
			String line;

			String lookup;

			while ((line = br.readLine()) != null) {
				String[] values =line.split(" ");
				datasetSize++;

				// some debug messages
				if( cmd.hasOption("verbose"))  if(lines%100000 == 0) LOG("read lines "+lines);

				Tuple2<Double,Double> Xi = new Tuple2<>(  normalizer.normalize(Double.parseDouble(values[0])), normalizer.normalize(Double.parseDouble(values[1])) );

				lookup = Xi.toString() ;
				sketch.update(lookup);

				lines++;
			}
			if( cmd.hasOption("verbose"))   LOG("reading input data finished. " + (lines)+" lines ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * this procedure initializes the centroids randomly within the projected inout-space
	 * @param centroids
	 */
	public static void initCentroidsRandomly(Tuple2<Double,Double>[] centroids){
		Random r = new java.util.Random();
		for( int c=0; c < centroids.length; c++ ) {
			// use zero - one normalizer and put the
			centroids[c] = new Tuple2<>( normalizer.getRandom(), normalizer.getRandom() );
			if( cmd.hasOption("verbose")) LOG("init centroid "+normalizer.normalize(c)+" => "+centroids[c]);
		}
	}

	/**
	 * this procedure initializes the centroids by the top heavy hitters found by reading the sketch
	 * @param centroids
	 * @throws Exception
	 */
	public static void initCentroidsHH(Tuple2<Double,Double>[] centroids) throws Exception {
		for( int c=0; c < centroids.length; c++ ) {
			CMEstimate topK = (CMEstimate) sketch.getHeavyHitters().getHeapArray()[c+1];
			if (topK != null) {
				String[] values = topK.key().replaceAll("[^-0-9,.E]", "").split(",");
				Tuple2<Double, Double> value = new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]));
				centroids[c] = new Tuple2<>( value.f0, value.f1 );
			} else {

				throw new Exception("invalid HH init for "+c);

			}
			if( cmd.hasOption("verbose")) LOG("init centroid "+normalizer.normalize(c)+" => "+centroids[c]);
		}
	}

	/**
	 * learn the model
	 */
	public static void learn(Tuple2<Double,Double>[] centroids) throws Exception {

		if( cmd.hasOption("init-randomly")){
			initCentroidsRandomly(centroids);
		} else {
			initCentroidsHH(centroids);
		}

		for( int i=0; i < numIterations; i++ ){
			if( cmd.hasOption("enumeration") ){
				updateClusterCentroidsWithEnumeration(centroids);
			} else {
				updateClusterCentroidsWithHeavyHitters(centroids);
			}

			if( cmd.hasOption("verbose")) {
				for (int k = 0; k < centroids.length; k++) {
					LOG("" + centroids[k]);
				}
			}//if
		}//for

	}

	/**
	 * updates the positions for all centroids in each iteration by enumerating the whole input-space
	 * @param centroids
	 */
	public static void updateClusterCentroidsWithHeavyHitters( Tuple2<Double,Double>[] centroids ){
		long freq;
		String lookup;
		long inputSpace=0;

		Tuple2<Double,Double>[] sums = new Tuple2[centroids.length];
		for( int l=0; l < sums.length; l++) sums[l] = new Tuple2<>(0.0,0.0);

		long[] counts = new long[centroids.length] ;


		for( int k=0; k < sketch.getHeavyHitters().getHeapArray().length-1; k++ ){
			CMEstimate topK = (CMEstimate)sketch.getHeavyHitters().getHeapArray()[k+1];

			if(topK!=null) {
				String[] values = topK.key().replaceAll("[^-0-9,.E]","").split(",");
				Tuple2<Double,Double> value = new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]) ) ;

				freq = topK.count();

				if(freq>0) {
					inputSpace++;

					int ibestCentroid = -1;
					double currDistance = Double.MAX_VALUE;
					for (int i = 0; i < centroids.length; i++) {
						double d = Math.sqrt(
								(value.f0 - centroids[i].f0) * (value.f0 - centroids[i].f0) +
										(value.f1 - centroids[i].f1) * (value.f1 - centroids[i].f1)  );
						if( d < currDistance){
							ibestCentroid = i;
							currDistance= d;
						}
					}//for


					// what is the closest center to that point?
					counts[ibestCentroid] += freq;

					// compute weighted sums dependent on counts from cm-sketch
					sums[ibestCentroid].f0 += value.f0*freq;
					sums[ibestCentroid].f1 += value.f1*freq;
				}//if
			}//if
		}//for
		// update centroids
		for (int i = 0; i < centroids.length; i++) {
			centroids[i].f0 = sums[i].f0 / counts[i];
			centroids[i].f1 = sums[i].f1 / counts[i];
			if( cmd.hasOption("verbose")) LOG("counted values for centroid "+i+" => "+counts[i]);

		}//for
		if( cmd.hasOption("verbose")) LOG("inputSpace size "+inputSpace);
	}


	/**
	 * updates the positions for all centroids in each iteration by enumerating the whole input-space
	 * @param centroids
	 */
	public static void updateClusterCentroidsWithEnumeration( Tuple2<Double,Double>[] centroids ){
		long freq;
		String lookup;
		long inputSpace=0;

		Tuple2<Double,Double>[] sums = new Tuple2[centroids.length];
		for( int l=0; l < sums.length; l++) sums[l] = new Tuple2<>(0.0,0.0);

		long[] counts = new long[centroids.length] ;

		// iterate through the whole input-space
		for (double x = (double) normalizer.getMin(); x < (double) normalizer.getMax(); x += (double) normalizer.getStep()) {
			for (double y = (double) normalizer.getMin(); y < (double) normalizer.getMax(); y += (double) normalizer.getStep()) {

				Tuple2<Double,Double> value = new Tuple2<>(normalizer.normalize(x),normalizer.normalize(y));
				lookup = "("+normalizer.normalize(x)+"," + normalizer.normalize(y) + ")";
				freq = sketch.get(lookup);

				if(freq>0) {
					inputSpace++;

					int ibestCentroid = -1;
					double currDistance = Double.MAX_VALUE;
					for (int i = 0; i < centroids.length; i++) {
						double d = Math.sqrt(
								(value.f0 - centroids[i].f0) * (value.f0 - centroids[i].f0) +
								(value.f1 - centroids[i].f1) * (value.f1 - centroids[i].f1)  );
						if( d < currDistance){
							ibestCentroid = i;
							currDistance= d;
						}
					}//for

					// what is the closest center to that point?
					counts[ibestCentroid] += freq;

					sums[ibestCentroid].f0 += value.f0*freq;
					sums[ibestCentroid].f1 += value.f1*freq;
				}//if

			}//for
		}//for

		// update centroids
		for (int i = 0; i < centroids.length; i++) {
			centroids[i].f0 = sums[i].f0 / counts[i];
			centroids[i].f1 = sums[i].f1 / counts[i];
			if( cmd.hasOption("verbose")) LOG("counted values for centroid "+i+" => "+counts[i]);

		}//for
		if( cmd.hasOption("verbose")) LOG("inputSpace size "+inputSpace);
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
						.withLongOpt("output")
						.withDescription("set the output path")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("o")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("centroids")
						.withDescription("set the number of centroids")
						.isRequired()
						.hasArg()
						.create("k")
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
						.create("p")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("heavyhitters")
						.withDescription("HeavyHitters")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("H")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("enumeration")
						.withDescription("enumerate input space for reconstruction")
								//.withValueSeparator('=')
						.create("e")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("print-sketch")
						.withDescription("only print sketch without running learning")
						.create("P")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("init-randomly")
						.withDescription("only print sketch without running learning")
						.create("r")
		);


		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;
		cmd = lvParser.parse(lvOptions, args );
		return cmd;
	}

	private static void LOG(String msg ){
		System.out.println("LOG: "+msg);
	}
}