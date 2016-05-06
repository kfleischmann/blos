package eu.blos.java.flink.sketch.api;


import eu.blos.java.sketches.DigestHashFunction;
import eu.blos.java.sketches.HashFunction;
import eu.blos.java.sketches.HashFunctionDescription;
import eu.blos.scala.inputspace.normalizer.Rounder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import java.io.Serializable;

public class SketchBuilder {
	private static final Log LOG = LogFactory.getLog(SketchBuilder.class);

	public static final int SKETCHTYPE_BLOOM_FILTER 	= 1;
	public static final int SKETCHTYPE_CM_SKETCH 		= 2;
	public static final String FIELD_DELIMITER			= ",";

	public static int[] ReduceSketchByFields(int ... keys ){
		return keys;
	}
	public static int[] Fields(int ... index  ){
		return index;
	}

	public static String constructKey( Object... fields ){
		String result="";
		for(int i=0; i < fields.length; i++) {
			result+=""+fields[i]+FIELD_DELIMITER;
		}//for
		// cleanup sketch field key (remove last delimiter if exists?)
		return result.trim().replaceAll(FIELD_DELIMITER+"$", "");
	}

	public static class FieldSketcherUDF implements SketcherUDF {

		private String fieldDelimiter;
		private int[] extractFields;
		private int valueIndex;
		private Double defaultValue=1.0;

		/**
		 *
		 * @param fieldDelimiter split each line by field-delimiter
		 * @param valueIndex field-index that should be used as emit value. default value is 1.0, that is was counted
		 * @param extractFields fields used for the hashing process
		 */
		public FieldSketcherUDF(String fieldDelimiter, int valueIndex, int ... extractFields ){
			this.fieldDelimiter = fieldDelimiter;
			this.extractFields = extractFields;
			this.valueIndex = valueIndex;
		}

		public FieldSketcherUDF(String fieldDelimiter,int ... extractFields ){
			this(fieldDelimiter, -1, extractFields);
		}

		private String extractFields(String[] record, int[] fields ){
			String result="";
			for(int i=0; i < fields.length; i++) {
				result+=record[fields[i]]+FIELD_DELIMITER;
			}//for

			// cleanup sketch field key (remove last delimiter if exists?)
			return "("+result.trim().replaceAll(FIELD_DELIMITER+"$", "")+")";
		}

		@Override
		public void sketch(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector, HashFunctionDescription hdesc ) {
			String[] fields = record.split(fieldDelimiter);
			String key = extractFields.length==0 ? record : extractFields(fields,extractFields);
			Double value = defaultValue;

			if(valueIndex!=-1){
				value=Double.parseDouble(fields[valueIndex]);
			}
			HashFunction[] hashFunctions = DigestHashFunction.generateHashfunctions((int) hdesc.d, hdesc.w);

			for( int i=0; i < hashFunctions.length; i++ ){
				long hash = hashFunctions[i].hash(key);
				collector.collect( new Tuple4<Long, Integer, Integer, Double>( hash, i, 1, value ) );
			}//for
		}
	}


	public static class DiscretizedFieldSketcherUDF implements SketcherUDF {

		private Double discretize(Double value){
			Double scale = Math.pow(10, resolution);
			return Math.round(value * scale) / scale;
		}

		private String fieldDelimiter;
		private int[] extractFields;
		private int valueIndex;
		private Double defaultValue=1.0;
		private int resolution=0;

		/**
		 *
		 * @param fieldDelimiter split each line by field-delimiter
		 * @param valueIndex field-index that should be used as emit value. default value is 1.0, that is was counted
		 * @param extractFields fields used for the hashing process
		 */
		public DiscretizedFieldSketcherUDF(String fieldDelimiter, int valueIndex, int resolution, int ... extractFields ){
			this.fieldDelimiter = fieldDelimiter;
			this.extractFields = extractFields;
			this.valueIndex = valueIndex;
			this.resolution = resolution;
		}

		public DiscretizedFieldSketcherUDF(String fieldDelimiter, int resolution, int ... extractFields ){
			this(fieldDelimiter, -1, resolution, extractFields);
		}

		private String extractFields(String[] record, int[] fields ){
			String result="";
			for(int i=0; i < fields.length; i++) {
				result+=discretize(Double.parseDouble(record[fields[i]])).toString()+FIELD_DELIMITER;
			}//for

			// cleanup sketch field key (remove last delimiter if exists?)
			return "("+result.trim().replaceAll(FIELD_DELIMITER+"$", "")+")";
		}

		@Override
		public void sketch(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector, HashFunctionDescription hdesc ) {
			String[] fields = record.split(fieldDelimiter);
			String key = extractFields.length==0 ? record : extractFields(fields,extractFields);
			Double value = defaultValue;

			if(valueIndex!=-1){
				value=Double.parseDouble(fields[valueIndex]);
			}
			HashFunction[] hashFunctions = DigestHashFunction.generateHashfunctions((int) hdesc.d, hdesc.w);
			for( int i=0; i < hashFunctions.length; i++ ){
				long hash = hashFunctions[i].hash(key);
				collector.collect( new Tuple4<Long, Integer, Integer, Double>( hash, i, 1, value ) );
			}//for
		}
	}



	public static Sketcher apply( String source, String dest, HashFunctionDescription hdesc, int type, int[] groupBy ){
		return new Sketcher( source, dest, hdesc, new FieldSketcherUDF( FIELD_DELIMITER ), type, groupBy );
	}

	public static Sketcher apply( String source, String dest, HashFunctionDescription hdesc, int type, SketcherUDF udf, int[] groupBy ){
		return new Sketcher( source, dest, hdesc, udf, type, groupBy );
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
					.reduce(new ReduceFunction<Tuple4<Long, Integer, Integer, Double>>() {
						@Override
						public Tuple4<Long, Integer, Integer, Double>
							reduce(Tuple4<Long, Integer, Integer, Double> left,
											Tuple4<Long, Integer, Integer, Double> right) throws Exception {
							return
								new Tuple4<Long, Integer, Integer, Double>(left.f0, left.f1, left.f2+right.f2, left.f3+right.f3 );
						}
					})
					.writeAsCsv(sketchDataPath + "/" + sketch.getDest(), "\n", FIELD_DELIMITER, FileSystem.WriteMode.OVERWRITE);

			// execute program
			env.execute("sketching source "+sketch.getDest() );
		}//for
	} // sketch5


	public static class SketchOperator implements Serializable, FlatMapFunction<String, Tuple4<Long, Integer, Integer, Double>> {

		private Sketcher sketcher;

		public SketchOperator( Sketcher sketcher ){
			this.sketcher = sketcher;
		}

		@Override
		public void flatMap(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector) throws Exception {
			sketcher.getUDF().sketch(record, collector, sketcher.getHashFunctionDescription() );
		}

	}
}
