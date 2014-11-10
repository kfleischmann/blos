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

	public static int[] ReduceSketchByFields(int ... keys ){
		return keys;
	}
	public static int[] Fields(int ... index  ){
		return index;
	}

	public static class DefaultSketcherUDF implements SketcherUDF {
		private String fieldDelimiter;
		private int[] extractFields;
		private int valueIndex;
		private Double defaultValue=1.0;

		/**
		 *
		 * @param fieldDelimiter split each line by field-delimiter
		 * @param valueIndex field-index that should be used as emit value. default value is 1.0
		 * @param extractFields fields used for the hashing process
		 */
		public DefaultSketcherUDF(String fieldDelimiter, int valueIndex, int ... extractFields ){
			this.fieldDelimiter = fieldDelimiter;
			this.extractFields = extractFields;
			this.valueIndex = valueIndex;

		}

		public DefaultSketcherUDF(String fieldDelimiter,int ... extractFields ){
			this(fieldDelimiter, -1, extractFields);
		}

		private String extractFields(String[] record, int[] fields ){
			String result="";
			for(int i=0; i < fields.length; i++) {
				result+=record[fields[i]]+" ";
			}//for
			return result.trim();
		}
		@Override
		public void sketch(String record, Collector<Tuple3<Long, Integer, Double>> collector, HashFunction[] hashFunctions ) {
			String[] fields = record.split(fieldDelimiter);
			String key = extractFields.length==0 ? record : extractFields(fields,extractFields);
			Double value = defaultValue;
			if(valueIndex!=-1){
				value=Double.parseDouble(fields[valueIndex]);
			}
			for( int i=0; i < hashFunctions.length; i++ ){
				long hash = hashFunctions[i].hash(key);
				collector.collect( new Tuple3<Long, Integer, Double>( hash, i, value ) );
			}//for
		}
	}
	public static Sketcher apply( String source, String dest, HashFunction[] hashfunction, int type, int[] groupBy ){
		return new Sketcher( source, dest, hashfunction, new DefaultSketcherUDF(","), type, groupBy );
	}

	public static Sketcher apply( String source, String dest, HashFunction[] hashfunctions, int type, SketcherUDF udf, int[] groupBy ){
		return new Sketcher( source, dest, hashfunctions, udf, type, groupBy );
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
				env	.readTextFile(preprocessedDataPath+"/"+sketch.getSource())
					.flatMap(new SketchOperator(sketch)) // do the hashing
					.groupBy(sketch.getGroupBy())  // reduce
					.reduce(new ReduceFunction<Tuple3<Long, Integer, Double>>() {
						@Override
						public Tuple3<Long, Integer, Double> reduce(Tuple3<Long, Integer, Double> left,
																	 Tuple3<Long, Integer, Double> right) throws Exception {
							return new Tuple3<Long, Integer, Double>(left.f0, left.f1, left.f2+right.f2 );
						}
					})
					.writeAsCsv(sketchDataPath + "/" + sketch.getDest(), "\n", ",", FileSystem.WriteMode.OVERWRITE);



			// execute program
			env.execute("sketching source "+sketch.getDest() );
		}//for
	} // sketch


	public static class SketchOperator extends FlatMapFunction<String, Tuple3<Long, Integer, Double>>  implements Serializable {

		private Sketcher sketcher;

		public SketchOperator( Sketcher sketcher ){
			this.sketcher = sketcher;
		}

		@Override
		public void flatMap(String record, Collector<Tuple3<Long, Integer, Double>> collector) throws Exception {
			sketcher.getUDF().sketch(record, collector, sketcher.getHashFunctions() );
		}
	}
}
