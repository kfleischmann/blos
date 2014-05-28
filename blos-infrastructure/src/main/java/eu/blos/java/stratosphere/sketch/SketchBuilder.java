package eu.blos.java.stratosphere.sketch;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;


public class SketchBuilder implements Program, ProgramDescription, Serializable {

    class PartialSketch extends MapFunction implements Serializable {
        private Collector<Record> collector = null;

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }
        public void close() throws Exception {
            Record r = new Record();

            //IntValue val = r.getField(3, IntValue.class);
            r.setField(0, new StringValue("some text"));

            collector.collect(r);


            super.close();
        }
        public void map(Record record, Collector<Record> out) {
            collector = out;
        }
    }


    class MergeSketch extends ReduceFunction implements Serializable {
        public void reduce( Iterator<Record> records, Collector<Record> out) {
            Record element = null;
            int sum = 0;
            while (records.hasNext()) {
                element = records.next();
                int cnt = element.getField(1, IntValue.class).getValue();
                sum += cnt;
            }

            element.setField(1, new IntValue(sum));
            out.collect(element);
        }
    }

    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 1 ? args[0] : "");
        String output    = (args.length > 2 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), "file:///home/kay/normalized_small.txt");

        // Operations on the data set go here
        // ...
        MapOperator mapper = MapOperator.builder(new PartialSketch())
                .input(source).build();


        FileDataSink sink = new FileDataSink( new CsvOutputFormat(), "file:///home/kay/output", mapper );

        CsvOutputFormat.configureRecordFormat(sink)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(StringValue.class, 0);

        return new Plan(sink);
    }


    public static void main(String[] args) throws Exception {
        String inputPath = "";
        String outputPath="";

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan( new SketchBuilder().getPlan(inputPath, outputPath) );

        //System.out.println("runtime:  " + runtime);
        executor.stop();

    }

    @Override
    public String getDescription() {
        return null;
    }
}