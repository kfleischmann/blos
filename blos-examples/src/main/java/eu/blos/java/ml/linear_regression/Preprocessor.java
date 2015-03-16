package eu.blos.java.ml.linear_regression;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Preprocessor {

	private static final Log LOG = LogFactory.getLog(Preprocessor.class);

	/**
	 * do the preprocessing for the sketching phase.
	 * reads the data from hdfs and prepare the data for the
	 * sketching phase
	 *
	 * expected input format
	 * label,feature
	 * 2d: y,x
	 *
	 * output
	 * sketch1 (k) => y^i * x_k^i
	 * sketch2 (j,k) => sum_{i=0}^N  x_j^i * x_k^i
	 * @param env
	 * @param inputPath
	 * @param outputPath
	 * @param args
	 * @throws Exception
	 */
	public static void transform(final ExecutionEnvironment env, String inputPath, String outputPath, String ... args ) throws Exception {
		LOG.info("start preprocessing phase");

		if(!outputPath.equals("stdout")) {
			new Path(outputPath).getFileSystem().delete(new Path(outputPath), true );
			new Path(outputPath).getFileSystem().mkdirs(new Path(outputPath));
		}

		DataSet<String> samples;

		// read samples
		if(!inputPath.equals("stdin")) {
			samples = env.readTextFile(inputPath);
		} else {
			Scanner input = new Scanner(System.in);
			List<String> stdinDataset = new ArrayList<String>();
			while(input.hasNext()) stdinDataset.add( input.next() );

			// get input data
			samples = env.fromElements(stdinDataset.toArray(new String[stdinDataset.size()]));
		}


		// output: (i, k, xk^i * y^i)
		DataSet<Tuple3<String, Integer,Double>> sketch1 =
				samples.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Double>>() {
					@Override
					public void flatMap(String s, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {
						// format: (sampleId,y,attributes)
						String[] fields = s.split((","));
						String sampleId = fields[0];
						String[] features = fields[2].split(" ");
						Double label = Double.parseDouble(fields[1]);

						collector.collect( new Tuple3<String,Integer,Double>( sampleId, 0, label ) );

						for(int k=1; k <= features.length; k++ ){
							Double value = label*Double.parseDouble(features[k-1]);
							collector.collect( new Tuple3<String,Integer,Double>( sampleId, k, value ) );
						}//for
					} // flatMap
				});

		// output: for each index j in x (i, k, xk^i * xj^i)
		DataSet<Tuple4<String, Integer, Integer,Double>> sketch2 =
				samples.flatMap(new FlatMapFunction<String, Tuple4<String, Integer, Integer, Double>>() {
					@Override
					public void flatMap(String s, Collector<Tuple4<String, Integer, Integer, Double>> collector) throws Exception {
						String[] fields = s.split((","));
						String sampleId = fields[0];
						String[] features = fields[2].split(" ");

						for(int j=0; j < features.length; j++ ) {
							Double value1 = Double.parseDouble(features[j]);

							collector.collect(new Tuple4<String, Integer, Integer, Double>(sampleId, j, 0, value1 ));

							for (int k = 1; k <= features.length; k++) {
								Double value2 = Double.parseDouble(features[k-1]);
								collector.collect(new Tuple4<String, Integer, Integer, Double>(sampleId, j, k, value1*value2));
							}//for
						}
					} // flatMap
				});
		sketch1.writeAsCsv( outputPath+"/sketch_labels", "\n", ",", FileSystem.WriteMode.OVERWRITE );
		sketch2.writeAsCsv( outputPath+"/sketch_samples", "\n", ",", FileSystem.WriteMode.OVERWRITE );

		// writing to std-output make no sense right now.

		// execute program
		env.execute("Preprocessing");


	}
}
