package eu.blos.java.stratosphere.sketch;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

import java.util.Iterator;

@SuppressWarnings("serial")
public class PDDBuilder {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = env.readTextFile(args[1]);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.mapPartition(new SketchBuilder())
						//.reduce( new SketchMerger)
						// group by the tuple field "0" and sum up tuple field "1"
						.groupBy(0)
						.sum(1);

		// emit result
		if(fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
		}

		// execute program
		env.execute("WordCount Example");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	public static final class SketchBuilder extends MapPartitionFunction<String, Tuple2<String, Integer>> {
		@Override
		public void mapPartition(Iterator<String> records, Collector<Tuple2<String, Integer>> out) throws Exception {
			while(records.hasNext()){
				// normalize and split the line
				String[] tokens = records.next().toLowerCase().split("\\W+");
				// emit the pairs
				for (String token : tokens) {
					if (token.length() > 0) {
						out.collect(new Tuple2<String, Integer>(token, 1));
					}
				}
			}
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: WordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WordCount <text path> <result path>");
		}
		return true;
	}
}