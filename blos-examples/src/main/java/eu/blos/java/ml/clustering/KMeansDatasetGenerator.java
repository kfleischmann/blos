package eu.blos.java.ml.clustering;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.clustering.KMeans;

/**
 * Generates data for the {@link KMeans} example program.
 */
public class KMeansDatasetGenerator {

	static {
		Locale.setDefault(Locale.US);
	}

	private static final String CENTERS_FILE = "centers";
	private static final String POINTS_FILE = "points";
	private static final long DEFAULT_SEED = 4650285087650871364L;
	private static final double DEFAULT_VALUE_RANGE = 100.0;
	private static final double RELATIVE_STDDEV = 0.08;
	private static final int DEFAULT_DIMENSIONALITY = 2;
	private static final DecimalFormat FORMAT = new DecimalFormat("#0.00");
	private static final char DELIMITER = ' ';
	private static final int DEFAULT_RESOLUTION = 2;

	/**
	 * Main method to generate data for the {@link KMeans} example program.
	 * <p>
	 * The generator creates to files:
	 * <ul>
	 * <li><code>&lt; output-path &gt;/points</code> for the data points
	 * <li><code>&lt; output-path &gt;/centers</code> for the cluster centers
	 * </ul>
	 *
	 * @param args
	 * <ol>
	 * <li>Int: Number of data points
	 * <li>Int: Number of cluster centers
	 * <li><b>Optional</b> String: Output path, default value is {tmp.dir}
	 * <li><b>Optional</b> Double: Standard deviation of data points
	 * <li><b>Optional</b> Double: Value range of cluster centers
	 * <li><b>Optional</b> Long: Random seed
	 * </ol>
	 */
	public static void main(String[] args) throws IOException {

		// check parameter count
		if (args.length < 2) {
			System.out.println("KMeansDataGenerator -points <num> -k <num clusters> [-output <output-path>] [-output-centroids] [-stddev <relative stddev>] [-range <centroid range>] [-seed <seed>]");
			System.exit(1);
		}

		// parse parameters

		final ParameterTool params = ParameterTool.fromArgs(args);
		final int numDataPoints = params.getInt("points", 0 );
		final int k = params.getInt("k");
		final String outDir = params.get("output", System.getProperty("java.io.tmpdir"));
		final String outCentroidsDir = params.get("output-centroids", outDir );
		final double stddev = params.getDouble("stddev", RELATIVE_STDDEV);
		final double range = params.getDouble("range", DEFAULT_VALUE_RANGE);
		final long firstSeed = params.getLong("seed", DEFAULT_SEED);
		final int dimensionality = params.getInt("dim", DEFAULT_DIMENSIONALITY);
		final int resolution = params.getInt("resolution", DEFAULT_RESOLUTION);

		// update resolution of the output format
		FORMAT.applyPattern( "#0."+StringUtils.repeat('0', resolution) );

		final double absoluteStdDev = stddev * range;
		final Random random = new Random(firstSeed);

		// the means around which data points are distributed
		final double[][] means = uniformRandomCenters(random, k, dimensionality, range);

		if( params.get("output") != null &&  params.get("output").length() > 0 ) {
			// write the points out
			BufferedWriter pointsOut = null;
			try {
				pointsOut = new BufferedWriter(new FileWriter(new File(outDir + "/" + POINTS_FILE)));
				StringBuilder buffer = new StringBuilder();

				double[] point = new double[dimensionality];
				int nextCentroid = 0;

				for (int i = 1; i <= numDataPoints; i++) {
					// generate a point for the current centroid
					double[] centroid = means[nextCentroid];
					for (int d = 0; d < dimensionality; d++) {
						point[d] = (random.nextGaussian() * absoluteStdDev) + centroid[d];
					}
					writePoint(point, buffer, pointsOut);
					nextCentroid = (nextCentroid + 1) % k;
				}
			} finally {
				if (pointsOut != null) {
					pointsOut.close();
				}
			}
			System.out.println("Wrote "+numDataPoints+" data points to "+outDir+"/"+POINTS_FILE);
		}//if


		if( (params.get("output") != null &&  params.get("output").length() > 0) ||
			(params.get("output-centroids") != null &&  params.get("output-centroids").length() > 0) ) {
			BufferedWriter centersOut = null;
			BufferedWriter centersOutInit = null;
			try {
				centersOut = new BufferedWriter(new FileWriter(new File(outCentroidsDir + "/" + CENTERS_FILE)));
				centersOutInit = new BufferedWriter(new FileWriter(new File(outCentroidsDir + "/init_" + CENTERS_FILE)));
				StringBuilder buffer = new StringBuilder();

				for (int i = 0; i < k; i++) {
					writeCenter(i + 1, means[i], buffer, centersOut);
				}//for

				double[][] centers = uniformRandomCenters(random, k, dimensionality, range);

				for (int i = 0; i < k; i++) {
					writeCenter(i + 1, centers[i], buffer, centersOutInit);
				}//for
			} finally {
				if (centersOut != null) {
					centersOut.close();
				}//if
				if (centersOutInit != null) {
					centersOutInit.close();
				}//if
			}

			System.out.println("Wrote "+k+" cluster centers to "+outCentroidsDir+"/"+CENTERS_FILE);
		}//if
	}

	private static double[][] uniformRandomCenters(Random rnd, int num, int dimensionality, double range) {
		final double halfRange = range / 2;
		final double[][] points = new double[num][dimensionality];

		for (int i = 0; i < num; i++) {
			for (int dim = 0; dim < dimensionality; dim ++) {
				points[i][dim] = (rnd.nextDouble() * range) - halfRange;
			}
		}
		return points;
	}

	private static void writePoint(double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
		buffer.setLength(0);

		// write coordinates
		for (int j = 0; j < coordinates.length; j++) {
			buffer.append(FORMAT.format(coordinates[j]));
			if(j < coordinates.length - 1) {
				buffer.append(DELIMITER);
			}
		}

		out.write(buffer.toString());
		out.newLine();
	}

	private static void writeCenter(long id, double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
		buffer.setLength(0);

		// write id
		buffer.append(id);
		buffer.append(DELIMITER);

		// write coordinates
		for (int j = 0; j < coordinates.length; j++) {
			buffer.append(FORMAT.format(coordinates[j]));
			if(j < coordinates.length - 1) {
				buffer.append(DELIMITER);
			}
		}

		out.write(buffer.toString());
		out.newLine();
	}
}