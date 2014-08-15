package eu.blos.java.stratosphere.sketch.api;


import eu.blos.java.algorithms.sketches.HashFunction;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

public class SketchBuilder {

	private static final Log LOG = LogFactory.getLog(SketchBuilder.class);

	public static final int SKETCHTYPE_BLOOM_FILTER 	= 1;
	public static final int SKETCHTYPE_CM_SKETCH 		= 2;

	public static Sketcher apply( String source, String dest, HashFunction[] hashfunctions, int type, SketcherUDF udf ){
		return new Sketcher( source, dest, hashfunctions, udf, type );
	}

	public static class DefaultSketcherUDF implements SketcherUDF {
		@Override
		public void sketch(String record, Collector<Tuple3<Long, Integer, Integer>> collector, HashFunction[] hashFunctions) {
			for( int i=0; i < hashFunctions.length; i++ ){
				collector.collect( new Tuple3<Long, Integer, Integer>( hashFunctions[i].hash(record.getBytes()), i, 1 ) );
			}//for
		}
	}

	public static Sketcher apply( String source, String dest, HashFunction[] hashfunctions, int type ){
		return new Sketcher( source, dest, hashfunctions, new DefaultSketcherUDF(), type );
	}

	/**
	 *
	 * @param preprocessedDataPath
	 * @param sketchDataPath
	 * @param mapper
	 */
	public static void sketch( final ExecutionEnvironment env, String preprocessedDataPath, String sketchDataPath, Sketcher... mapper ) throws Exception {
		LOG.info("start building sketches for learning phase");

		// prepare
		new Path(sketchDataPath).getFileSystem().delete(new Path(sketchDataPath), true );
		new Path(sketchDataPath).getFileSystem().mkdirs(new Path(sketchDataPath));

		for( Sketcher sketch : mapper ){

			// TODO: possible optimization - bloomfilter hashes can be reduced with groupBy(0), instean groupBy(0,1)

			env.readTextFile(preprocessedDataPath+"/"+sketch.getSource())
					.flatMap(new SketchOperator(sketch)) // do the hashing
					.groupBy(0,1)  // reduce
					.reduce(new ReduceFunction<Tuple3<Long, Integer, Integer>>() {
						@Override
						public Tuple3<Long, Integer, Integer> reduce(Tuple3<Long, Integer, Integer> left,
																	 Tuple3<Long, Integer, Integer> right) throws Exception {
							return new Tuple3<Long, Integer, Integer>(left.f0, left.f1, left.f2+right.f2 );
						}
					})
					.writeAsCsv(sketchDataPath + "/" + sketch.getDest(), "\n", ",", FileSystem.WriteMode.OVERWRITE);

			// execute program
			env.execute("sketching source "+sketch.getDest() );
		}//for
	} // sketch


	public static class SketchOperator extends FlatMapFunction<String, Tuple3<Long, Integer, Integer>>  implements Serializable {

		private Sketcher sketcher;

		public SketchOperator( Sketcher sketcher ){
			this.sketcher = sketcher;
		}

		@Override
		public void flatMap(String record, Collector<Tuple3<Long, Integer, Integer>> collector) throws Exception {
			sketcher.getUDF().sketch(record, collector, sketcher.getHashFunctions() );
		}
	}
}
