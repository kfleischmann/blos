package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.ZeroOneNormalizer;
import eu.blos.java.flink.helper.DataSetReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

public class Preprocessor {

	private static final Log LOG = LogFactory.getLog(Preprocessor.class);

	public static String FIELD_SEPARATOR = ",";
	public static String VALUE_SEPARATOR = " ";

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
	public static void transform(final ExecutionEnvironment env, String inputPath, String outputPath, final FieldNormalizer normalizer, String ... args ) throws Exception {
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
			samples = DataSetReader.fromStdin(env);
		}


		// output: (i, k, xk^i * y^i)
		DataSet<Tuple4<String, Integer,Double, Integer>> sketch1 =
				samples.flatMap(new FlatMapFunction<String, Tuple4<String, Integer, Double, Integer>>() {
					@Override
					public void flatMap(String s, Collector<Tuple4<String, Integer, Double, Integer>> collector) throws Exception {
						// format: (sampleId,y,attributes)
						String[] fields = s.split(( FIELD_SEPARATOR ));
						String sampleId = fields[0];
						String[] features = fields[2].split( VALUE_SEPARATOR );
						Double label = Double.parseDouble(fields[1]);

						//k==0
						//collector.collect( new Tuple4<String,Integer,Double, Integer>( sampleId, 0, label ) );

						//k>0
						// for each sample i, feature k emit y^i * x_k^i
						for(int k=0; k < features.length; k++ ){
							Double value = (k == 0? label : label*Double.parseDouble(features[k]) );
							Integer normalized = normalizer.normalize(value.doubleValue());

							collector.collect( new Tuple4<String,Integer,Double, Integer>( sampleId, k, value, normalized ) );
						}//for
					} // flatMap
				});

		// output: for each index (i, k, j,) =>  xk^i * xj^i
		DataSet<Tuple5<String, Integer, Integer,Double, Integer>> sketch2 =
				samples.flatMap(new FlatMapFunction<String, Tuple5<String, Integer, Integer, Double, Integer>>() {
					@Override
					public void flatMap(String s, Collector<Tuple5<String, Integer, Integer, Double, Integer>> collector) throws Exception {
						String[] fields = s.split(( FIELD_SEPARATOR ));
						String sampleId = fields[0];
						String[] features = fields[2].split( VALUE_SEPARATOR );

						for(int j=0; j <= features.length; j++ ) {
							//Double jvalue = Double.parseDouble(features[j]);


							for (int k = 0; k <= features.length; k++) {

								Double value = (j==0? 1.0 : Double.parseDouble(features[j-1]) )*(k == 0? 1.0 :  Double.parseDouble(features[k-1]) );
								Integer normalized = normalizer.normalize(value.doubleValue());

								collector.collect(
											new Tuple5<String, Integer, Integer, Double, Integer>(sampleId, j, k, value, normalized )
											);
							}//for

						}//for
					} // flatMap
				});
		sketch1.writeAsCsv( outputPath+"/sketch_labels", "\n", FIELD_SEPARATOR, FileSystem.WriteMode.OVERWRITE );
		sketch2.writeAsCsv( outputPath+"/sketch_samples", "\n", FIELD_SEPARATOR, FileSystem.WriteMode.OVERWRITE );

		// writing to std-output make no sense right now.

		// execute program
		env.execute("Preprocessing");


	}
}
