package eu.blos.java.stratosphere.sketch;

import eu.blos.java.algorithms.sketches.PDDCMSketch;
import eu.blos.java.api.common.PDDSet;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.io.SerializedInputFormat;
import eu.stratosphere.api.common.io.SerializedOutputFormat;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.io.Serializable;

public class PDDReader  implements Program, ProgramDescription, Serializable {

    public static class MyPDDReader extends MapFunction implements Serializable {

        public MyPDDReader(){
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        public void close() throws Exception {
            super.close();
        }


        public void map(Record record, Collector<Record> out) throws IllegalAccessException, InstantiationException {

            PDDSet pddset = PDDSet.class.newInstance();
            record.getFieldInto(0, pddset);

            pddset.print();

            PDDCMSketch cm = (PDDCMSketch)pddset.getPDDs().get(0);

            System.out.println("MAP!: "+cm.get("test"));
        }
    }


    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 0 ? args[0] : "");
        String output    = (args.length > 1 ? args[1] : "");

        FileDataSource source = new FileDataSource(new SerializedInputFormat(), dataInput );

        // Operations on the data set go here
        MapOperator reader = MapOperator.builder(new MyPDDReader())
                .input(source)
                .name("local sketches")
                .build();



        FileDataSink sink = new FileDataSink( new SerializedOutputFormat(), output, reader );

        /*CsvOutputFormat.configureRecordFormat(sink)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(StringValue.class, 0);*/


        return new Plan(sink);
    }

    @Override
    public String getDescription() {
        return null;
    }

}
