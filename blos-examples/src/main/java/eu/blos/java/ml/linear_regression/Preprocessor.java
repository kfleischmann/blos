package eu.blos.java.ml.linear_regression;

import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.core.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Preprocessor {

	private static final Log LOG = LogFactory.getLog(Preprocessor.class);

	/**
	 * do the preprocessing for the sketching phase. reads the data from hdfs and prepare the data for the
	 * sketching phase
	 *
	 * @param env
	 * @param inputPath
	 * @param outputPath
	 * @param args
	 * @throws Exception
	 */
	public static void process(final ExecutionEnvironment env, String inputPath, String outputPath, String ... args ) throws Exception {
		LOG.info("start preprocessing phase");

		// prepare
		new Path(outputPath).getFileSystem().delete(new Path(outputPath), true );
		new Path(outputPath).getFileSystem().mkdirs(new Path(outputPath));

	}
}
