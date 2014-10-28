package eu.blos.java.ml.linear_regression;

import eu.blos.java.ml.random_forest.RFBuilder;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class Preprocessor {

	private static final Log LOG = LogFactory.getLog(Preprocessor.class);

	/**
	 * do the preprocessing for the sketching phase. reads the data from hdfs and prepare the data for the
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

		// prepare
		new Path(outputPath).getFileSystem().delete(new Path(outputPath), true );
		new Path(outputPath).getFileSystem().mkdirs(new Path(outputPath));

		// read samples
		DataSet<String> samples = env.readTextFile(inputPath);

		// output: (k, xk^i * y^i)
		DataSet<Tuple2<Integer,Double>> sketch1 =
				samples.flatMap(new FlatMapFunction<String, Tuple2<Integer, Double>>() {
					@Override
					public void flatMap(String s, Collector<Tuple2<Integer, Double>> collector) throws Exception {
						String[] fields = s.split((","));
						String[] features = fields[1].split(" ");
						Double label = Double.parseDouble(fields[0]);
						for(int k=0; k < features.length; k++ ){
							Double value = label*Double.parseDouble(features[k]);
							collector.collect( new Tuple2<Integer,Double>( k, value ) );
						}//for
					} // flatMap
				});

		// output: (k, xk^i * y^i)
		DataSet<Tuple3<Integer, Integer,Double>> sketch2 =
				samples.flatMap(new FlatMapFunction<String, Tuple3<Integer, Integer, Double>>() {
					@Override
					public void flatMap(String s, Collector<Tuple3<Integer, Integer, Double>> collector) throws Exception {
						String[] fields = s.split((","));
						String[] features = fields[1].split(" ");

						for(int j=0; j < features.length; j++ ) {
							Double value1 = Double.parseDouble(features[j]);
							for (int k = 0; k < features.length; k++) {
								Double value2 = Double.parseDouble(features[k]);
								collector.collect(new Tuple3<Integer, Integer, Double>(j, k, value1*value2));
							}//for
						}
					} // flatMap
				});



		// group by dimension k
		sketch1.groupBy(0).reduce(new ReduceFunction<Tuple2<Integer, Double>>() {
			@Override
			public Tuple2<Integer, Double> reduce(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) throws Exception {
				return new Tuple2<Integer,Double>(t1.f0, t1.f1+t2.f1);
			}
		});

		// group by dimension j,k
		sketch2.groupBy(0,1).reduce(new ReduceFunction<Tuple3<Integer, Integer, Double>>() {
			@Override
			public Tuple3<Integer, Integer, Double> reduce(Tuple3<Integer, Integer, Double> t1, Tuple3<Integer, Integer, Double> t2) throws Exception {
				return new Tuple3<Integer, Integer,Double>(t1.f0, t1.f1, t1.f2+t2.f2);
			}
		});

		sketch1.writeAsCsv( outputPath+"/sketch1", "\n", ",", FileSystem.WriteMode.OVERWRITE );
		sketch1.writeAsCsv( outputPath+"/sketch2", "\n", ",", FileSystem.WriteMode.OVERWRITE );
	}
}
