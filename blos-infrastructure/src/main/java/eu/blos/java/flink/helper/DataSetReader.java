package eu.blos.java.flink.helper;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DataSetReader {
	private static DataSet<String> stdinDatast = null;

	public static DataSet<String> fromStdin(final ExecutionEnvironment env){
		if(stdinDatast == null ) {
			List<String> stdinDataset = new ArrayList<String>();
			Scanner input = new Scanner(System.in);
			while (input.hasNext()) stdinDataset.add(input.next());
			stdinDatast =  env.fromElements(stdinDataset.toArray(new String[stdinDataset.size()]));
			return stdinDatast;
		} else {
			return stdinDatast;
		}
	}
}
