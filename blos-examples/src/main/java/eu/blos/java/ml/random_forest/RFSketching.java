package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.HashFunction;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.util.Collector;

import java.util.Iterator;


public class RFSketching {
	public static boolean fileOutput =  false;

    public static void main(String[] args) throws Exception {

        String inputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final HashFunction[] hashfunctions = HashFunction.generateHashfunctions(5, 1000000 );


		// get input data
		DataSet<String> text = env.readTextFile(inputPath);

		DataSet<Tuple4<String, Long, Integer, Integer>> hashed =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap( new SketchBuilder(hashfunctions) );


		// emit result
		if(fileOutput) {
			hashed.writeAsCsv(outputPath, "\n", " ");
		} else {
			hashed.print();
		}

		// execute program
		env.execute("WordCount Example");

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	public static final class SketchBuilder extends FlatMapFunction<String, Tuple4<String, Long, Integer, Integer>> {
		public static HashFunction[] hashfunctions;
		public SketchBuilder( HashFunction[] hf ){
			hashfunctions = hf;
		}
		@Override
		public void flatMap(String value, Collector<Tuple4<String, Long, Integer, Integer>> out) {
			// normalize and split the line
			String[] values = value.toLowerCase().split("\\W+");

			for(int d=0; d < hashfunctions.length; d++ ){
				HashFunction hf = hashfunctions[d];

				String key = values[0]+"-splitcandidate";

				out.collect(new Tuple4<String, Long, Integer, Integer>(key, hf.hash((long)key.hashCode()), d, 1));
			}//for
		}
	}
}
