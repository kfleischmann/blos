package eu.blos.java.ml.clustering;

import eu.blos.java.algorithms.sketches.fieldnormalizer.RoundNormalizer;
import eu.blos.scala.algorithms.sketches.CMEstimate;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
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

		try {
			cmd = parseArguments(args);
		} catch (Exception e) {
			LOG(e.getMessage());
			lvFormater.printHelp("SketchedKMeans", lvOptions);
			return;
		}

		if (cmd != null && (cmd.hasOption('h') || (cmd.getArgs().length == 0 && cmd.getOptions().length == 0))) {
			lvFormater.printHelp("SketchedKMeans", lvOptions);
			return;
		}

		numCentroids = Integer.parseInt(cmd.getOptionValue("centroids"));
		numHeavyHitters = Integer.parseInt(cmd.getOptionValue("num-heavyhitters"));
		centroids = new Tuple2[numCentroids];

		// make it possible to read from stdin
		InputStreamReader is = null;
		if (cmd.getOptionValue("input").toLowerCase().equals("stdin")) {
			is = new InputStreamReader(System.in);
		} else {
			is = new FileReader(new File(cmd.getOptionValue("input")));
		}

		buildSketches(is);


		if (cmd.hasOption("print-sketch") || cmd.hasOption("verbose")) {
			// write output
			if (cmd.hasOption("output")) {
				sketch.display(System.out);
			} else {
				PrintStream out = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/sketch"));
				sketch.display(out);
				out.close();
			}

		}//if

		if (cmd.hasOption("verbose") || cmd.hasOption("print-sketch")) {
			if (!cmd.hasOption("output")) {
				System.out.println("num-heavyhitters");
				for (int k = 1; k < sketch.getHeavyHitters().getHeapArray().length; k++) {
					CMEstimate topK = (CMEstimate) sketch.getHeavyHitters().getHeapArray()[k];
					if (topK != null) {
						String[] values = topK.key().replaceAll("[^-0-9,.E]", "").split(",");
						Tuple2<Double, Double> d = new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]));

						System.out.println("" + k + " " + d.f0 + " " + d.f1);
					}//if
				}//for
				System.out.println("");
			} else {
				PrintStream out = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/heavyhitters"));
				for (int k = 1; k < sketch.getHeavyHitters().getHeapArray().length; k++) {
					CMEstimate topK = (CMEstimate) sketch.getHeavyHitters().getHeapArray()[k];
					if (topK != null) {
						String[] values = topK.key().replaceAll("[^-0-9,.E]", "").split(",");
						Tuple2<Double, Double> d = new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]));

						out.println("" + k + " " + d.f0 + " " + d.f1);
					}//if
				}//for
				out.println("");
				out.close();
			}
		}//if


		if (cmd.hasOption("discover-inputspace")) {
			reconstructInputSpace();
		}

		if (!cmd.hasOption("skip-learning")) {

			// execute learning for enumeration
			if (cmd.hasOption("enumeration")) {

				learnByEnum(centroids);

				// write output
				if (cmd.hasOption("output")) {
					PrintStream out = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/centers-enum"));
					for (int k = 0; k < centroids.length; k++) {
						out.println(k + " " + centroids[k].f0 + " " + centroids[k].f1);
					}//for
					out.close();

				} else {
					for (int k = 0; k < centroids.length; k++) {
						System.out.println(k + " " + centroids[k].f0 + " " + centroids[k].f1);
					}//for
				}
			}//if

			if (cmd.hasOption("heavyhitters")) {

				learnByHH(centroids);

				// write output
				if (cmd.hasOption("output")) {
					PrintStream out = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/centers-hh"));
					for (int k = 0; k < centroids.length; k++) {
						out.println(k + " " + centroids[k].f0 + " " + centroids[k].f1);
					}//for
					out.close();

				} else {
					for (int k = 0; k < centroids.length; k++) {
						System.out.println(k + " " + centroids[k].f0 + " " + centroids[k].f1);
					}//for
				}

			}//if
		}//if

	} // main()

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

		DecimalFormat df = new DecimalFormat("#.###");

		LOG("w="+sketch.w());
		LOG("d="+sketch.d());
		LOG("size(mb)="+ df.format(total_size/1024.0/1024.0 ));

		double max=0.0;
		double min=0.0;

		long lines=0;
		try (BufferedReader br = new BufferedReader( is )) {
			String line;

			String lookup;

			PrintStream out = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/hashed"));

			while ((line = br.readLine()) != null) {
				String[] values =line.split(" ");
				datasetSize++;

				// some debug messages
				if( cmd.hasOption("verbose"))  if(lines%100000 == 0) LOG("read lines "+lines);

				Tuple2<Double,Double> Xi = new Tuple2<>(  normalizer.normalize(Double.parseDouble(values[0])), normalizer.normalize(Double.parseDouble(values[1])) );

				lookup = Xi.toString() ;
				sketch.update(lookup);

				if( cmd.hasOption("show-hashed-values")){
					if( cmd.hasOption("output")) {
						out.println(lookup);
					} else {
					}
				}
				lines++;
			}
			out.close();

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
	 * this procedure initializes the centroids by the top [c] heavy hitters found by reading the sketch
	 * @param centroids
	 * @throws Exception
	 */
	public static void initCentroidsHH(Tuple2<Double,Double>[] centroids) throws Exception {
		for( int c=0; c < centroids.length; c++ ) {
			CMEstimate topK = (CMEstimate) sketch.getHeavyHitters().getHeapArray()[c+1];
			if (topK != null) {
				String[] values = topK.key().replaceAll("[^-0-9,.E]", "").split(",");
				centroids[c] =  new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]));
			} else {

				throw new Exception("invalid HH init for "+c);

			}
			if( cmd.hasOption("verbose")) LOG("init centroid "+normalizer.normalize(c)+" => "+centroids[c]);
		}
	}

	/**
	 * read centroids from input file and init centroids with its values
	 * @param centroids
	 * @param file
	 */
	public static void initCentroidsbyFile(Tuple2<Double,Double>[] centroids, String file ){
		try {
			InputStreamReader is = new FileReader(new File(file));
			BufferedReader br = new BufferedReader( is );
			String line;
			int c=0;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(" ");
				centroids[c] = new Tuple2<>(Double.parseDouble(values[1]), Double.parseDouble(values[2]));
				System.out.println("pre-init:"+centroids[c] );
				c++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
		}
	}

	/**
	 * learn the model
	 */
	public static void learnByEnum(Tuple2<Double,Double>[] centroids) throws Exception {

		if( cmd.hasOption("init-hh")){
			initCentroidsHH(centroids);
			if( cmd.hasOption("verbose")) LOG("init centroid by hh");
		}
		else if(cmd.hasOption("init-file")){
			initCentroidsbyFile(centroids, cmd.getOptionValue("init-file"));
			if( cmd.hasOption("verbose")) LOG("init centroid by file");
		}
		else {
			initCentroidsRandomly(centroids);
			if( cmd.hasOption("verbose")) LOG("init centroid randomly");
		}


		// -----------------------
		// LEARN BY ENUMERATION
		// -----------------------
		for (int i = 0; i < numIterations; i++) {
			if (cmd.hasOption("verbose")) System.out.println("Start iteration " + i);
			updateClusterCentroidsWithEnumeration(centroids);
			if (cmd.hasOption("verbose")) {
				for (int k = 0; k < centroids.length; k++) {
					LOG("" + centroids[k]);
				}
			}//if
		}//for
	}


	/**
	 * learn the model
	 */
	public static void learnByHH(Tuple2<Double,Double>[] centroids) throws Exception {

		if( cmd.hasOption("init-hh")){
			initCentroidsHH(centroids);
			if( cmd.hasOption("verbose")) LOG("init centroid by hh");
		}
		else if(cmd.hasOption("init-file")){
			initCentroidsbyFile(centroids, cmd.getOptionValue("init-file"));
			if( cmd.hasOption("verbose")) LOG("init centroid by file");
		}
		else {
			initCentroidsRandomly(centroids);
			if( cmd.hasOption("verbose")) LOG("init centroid randomly");
		}


		// -----------------------
		// LEARN BY HEAVY HITTERS
		// -----------------------
		for (int i = 0; i < numIterations; i++) {
			if (cmd.hasOption("verbose")) System.out.println("Start iteration " + i);
			updateClusterCentroidsWithHeavyHitters(centroids);
			if (cmd.hasOption("verbose")) {
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
			if( cmd.hasOption("verbose")) LOG("HH: counted values for centroid "+i+" => "+counts[i]);

		}//for
		if( cmd.hasOption("verbose")) LOG("inputSpace size "+inputSpace);
	}


	/**
	 * updates the positions for all centroids in each iteration by enumerating the whole input-space
	 * @param centroids
	 */
	public static void updateClusterCentroidsWithEnumeration( Tuple2<Double,Double>[] centroids ) throws FileNotFoundException {
		long freq;
		String lookup;
		long inputSpace=0;

		Tuple2<Double,Double>[] sums = new Tuple2[centroids.length];
		for( int l=0; l < sums.length; l++) sums[l] = new Tuple2<>(0.0,0.0);

		long[] counts = new long[centroids.length] ;

		PrintStream out = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/query-input-space"));

		// iterate through the whole input-space
		for (double y = (double) normalizer.getMax(); y >= (double) normalizer.getMin(); y -= (double) normalizer.getStep()) {
			for (double x = (double) normalizer.getMin(); x <= (double) normalizer.getMax(); x += (double) normalizer.getStep()) {

				Tuple2<Double,Double> value = new Tuple2<>(normalizer.normalize(x),normalizer.normalize(y));
				lookup = "("+normalizer.normalize(x)+"," + normalizer.normalize(y) + ")";
				freq = sketch.get(lookup);

				out.print(freq+" ");

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
			out.println();

		}//for

		// update centroids
		for (int i = 0; i < centroids.length; i++) {
			centroids[i].f0 = sums[i].f0 / counts[i];
			centroids[i].f1 = sums[i].f1 / counts[i];
			if( cmd.hasOption("verbose")) LOG("ENUM: counted values for centroid "+i+" => "+counts[i]);

		}//for
		if( cmd.hasOption("verbose")) LOG("inputSpace size "+inputSpace);

		out.close();
	}


	/**
	 *
	 * @throws FileNotFoundException
	 */
	public static void reconstructInputSpace() throws FileNotFoundException {
		String lookup;
		HashMap<String, Long> hh = new HashMap<>();

		for( int k=0; k < sketch.getHeavyHitters().getHeapArray().length-1; k++ ) {
			CMEstimate topK = (CMEstimate) sketch.getHeavyHitters().getHeapArray()[k + 1];
			if (topK != null) {
				hh.put(topK.key(), topK.count());
			}//if
		}//for


		long freq;
		long elements=0;
		long total_elements=normalizer.getTotalElements()*normalizer.getTotalElements();
		int percentage = -1;
		PrintStream out_enum = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/enumerated-input-space"));
		PrintStream out_hh = new PrintStream(new FileOutputStream(cmd.getOptionValue("output") + "/hh-input-space"));

		// iterate through the whole input-space
		for (double y = (double) normalizer.getMax(); y >= (double) normalizer.getMin(); y -= (double) normalizer.getStep()) {
			for (double x = (double) normalizer.getMin(); x <= (double) normalizer.getMax(); x += (double) normalizer.getStep()) {
				elements++;

				// enumerate
				Tuple2<Double,Double> value = new Tuple2<>(normalizer.normalize(x),normalizer.normalize(y));
				lookup = "("+normalizer.normalize(x)+"," + normalizer.normalize(y) + ")";
				freq = sketch.get(lookup);
				out_enum.print(freq+" ");

				// hh
				if( hh.containsKey(lookup) ) {
					freq = hh.get(lookup);
					out_hh.print(freq+" ");
				}else {
					out_hh.print(0+" ");
				}

				if( (int)(((float)elements/(float)total_elements)*100) != percentage){
					percentage=	(int)(((float)elements/(float)total_elements)*100);
					System.out.println(percentage+"% discovered" );
				}
			}//for
			out_enum.println();
			out_hh.println();
		}//for
		out_enum.close();
		out_hh.close();
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
						.withLongOpt("skip-learning")
						.withDescription("skip-learning")
						.create("S")
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
						.withLongOpt("num-heavyhitters")
						.withDescription("num-heavyhitters")
						.isRequired()
								//.withValueSeparator('=')
						.hasArg()
						.create("N")
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
						.withLongOpt("heavyhitters")
						.withDescription("use hh for reconstruction")
								//.withValueSeparator('=')
						.create("H")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("print-sketch")
						.withDescription("print sketch")
						.create("P")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("discover-inputspace")
						.withDescription("discover input-space for reconstruction")
						.create("D")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("init-hh")
						.withDescription("init centroids based on the first hh")
						.create("I")
		);


		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("show-hashed-values")
						.withDescription("write out hashed values std or /hashed")
						.create("V")
		);

		lvOptions.addOption(
				OptionBuilder
						.withLongOpt("init-file")
						.hasArg()
						.withDescription("init centroids with provided ile")
						.create("F")
		);


		CommandLineParser lvParser = new BasicParser();
		CommandLine cmd = null;
		cmd = lvParser.parse(lvOptions, args );
		return cmd;
	}

	private static void LOG(String msg ){
		System.out.println(msg);
	}
}