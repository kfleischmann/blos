package eu.blos.java.flink.helper;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class StatisticsBuilder implements Serializable {

	public static void run(final ExecutionEnvironment env, String inputPath, String outputPath, final SampleFormat format ) throws Exception {

		// read samples
		DataSet<Tuple3<String, Integer, Integer>> statistics = env.readTextFile(inputPath).map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
			@Override
			public Tuple3<String, Integer, Integer> map(String value) throws Exception {
				String[] values = value.split(format.getFieldDelimiter());
				String label = format.getLabelPosition() == -1 ? "" : values[format.getLabelPosition()];
				Integer featuresCount = values[format.getFeaturesPosition()].split(format.getFeatureDelimiter()).length;

				return new Tuple3<String, Integer, Integer>(label, featuresCount, 1);
			}
		}).reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
			@Override
			public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {
				String labels="";
				if(format.getLabelPosition() > -1 ) {
					String[] a = value1.f0.split(" ");
					String[] b = value2.f0.split(" ");
					List<String> list = new ArrayList<String>(Arrays.asList(a));
					list.addAll(Arrays.asList(b));
					Object[] c = list.toArray();
					labels = StringUtils.join(c, " ");
				}
				return new Tuple3<String, Integer, Integer>(labels, Math.max(value1.f1, value2.f1), value1.f2 + value2.f2);
			}
		});

		statistics.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("reading statistics");
	}


	public static DataSetStatistics read(final ExecutionEnvironment env, String filePath ) throws IOException {
		Path f = new Path(filePath);
		FileSystem fs = FileSystem.get(f.toUri());

		InputStream is = fs.open(f);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		DataSetStatistics statistics = new DataSetStatistics();

		String line;
		while ((line = reader.readLine()) != null) {
			String[] values = line.split(",");
			String[] labels = values[0].split(" ");


			statistics.setLabels(labels);
			statistics.setFeatureCount(Integer.parseInt(values[1]));
			statistics.setSampleCount(Integer.parseInt(values[2]));
		}
		reader.close();
		is.close();

		return statistics;
	}
}
