package eu.blos.java.stratosphere.sketch;

import eu.blos.java.api.common.PDDSet;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.functions.MapPartitionFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.MapPartitionOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;


public class MapReduceJob implements Program, ProgramDescription, Serializable {

    public static class PartialSketch extends MapPartitionFunction implements Serializable {
		public static PDDSet set = null;

        public PartialSketch(PDDSet t ){
			this.set = t;
        }

		@Override
		public void mapPartition(Iterator<Record> recordIterator, Collector<Record> recordCollector) throws Exception {
			this.set.alloc();
		}
	}


    public static class MergeSketch extends ReduceFunction implements Serializable  {
        public void reduce( Iterator<Record> records, Collector<Record> out) {
            Record element = null;
            int sum = 0;

            while (records.hasNext()) {
                element = records.next();
                String cnt = element.getField(0, StringValue.class).getValue();
                sum += 1;
                System.out.println("text: "+cnt);
            }

            Record r = new Record(new StringValue("test"), new IntValue(sum) );
            out.collect(r);
        }
    }

    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 1 ? args[0] : "");
        String output    = (args.length > 2 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), "file:///home/kay/normalized_small.txt");


        // Operations on the data set go here
        // ...
        MapPartitionOperator sketcher = MapPartitionOperator.builder( new PartialSketch(null) )
                .input(source)
                .name("local sketches")
                .build();

        //sketcher.setDegreeOfParallelism(5);


        ReduceOperator merger = ReduceOperator.builder( MergeSketch.class )
                .input(sketcher)
                .name("merge sketches")
                .build();


        FileDataSink sink = new FileDataSink( new CsvOutputFormat(), "file:///home/kay/output3", merger );

        CsvOutputFormat.configureRecordFormat(sink)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);

        return new Plan(sink);
    }


    public static void main(String[] args) throws Exception {
        String inputPath = "";
        String outputPath="";

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan( new MapReduceJob().getPlan(inputPath, outputPath) );

        //System.out.println("runtime:  " + runtime);
        executor.stop();
    }

    @Override
    public String getDescription() {
        return null;
    }
}