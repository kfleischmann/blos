package eu.blos.java.stratosphere.sketch;

import eu.blos.java.api.common.PDD;
import eu.blos.java.api.common.PDDSet;
import eu.blos.java.api.common.Sketcher;
import eu.blos.scala.algorithms.PDDHistogram;
import eu.blos.scala.algorithms.sketches.PDDCMSketch;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.io.BinaryOutputFormat;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.scala.operators.BinarySerializedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;


public class PDDBuilder implements Program, ProgramDescription, Serializable {

    static final long serialVersionUID = 1L;

    /**
     * context for the PDD (e.g. hash functions) for local allocation
     * very important to allow a merging phase after the skeching phase
     */

    /**
     * actual code that do the skeching
     */


    public PDDBuilder(){

    }


    /**
     * construct the partial PDD during a mapping phase
     */

    public static class PDDCarrier extends MapFunction implements Serializable {

        private Collector<Record> collector = null;

        private PDDSet set;

        public PDDCarrier(PDDSet set){
            this.set = set;
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            System.out.println("open mapper");

            set.alloc();
            // allocate
        }

        public void close() throws Exception {
            Record r = new Record();
            r.setField(0, set );
            collector.collect(r);
            super.close();
        }


        public void map(Record record, Collector<Record> out) {
            if(collector==null) collector = out;

            System.out.println("map");
            // execute
        }
    }

    /**
     * merge partial sketches into one
     */
    public static class PDDCombiner extends ReduceFunction implements Serializable {

        public PDDCombiner(){
        }

        public void reduce( Iterator<Record> records, Collector<Record> out) throws Exception {
            System.out.println("reduce!");

            PDDSet global_PDD = null;
            Record element = null;

            while (records.hasNext()) {
                element = records.next();

                PDDSet pddset = PDDSet.class.newInstance();
                element.getFieldInto(0, pddset );

                // prepare global PDD
                if(global_PDD == null ) {
                    global_PDD = pddset;
                } else {
                    global_PDD.mergeWith(pddset);
                }
            }

            //global_PDD.print();
            out.collect(new Record(new StringValue("test")));
        }
    }


    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 0 ? args[0] : "");
        String output    = (args.length > 1 ? args[1] : "");

        PDDCMSketch pdd1 = new PDDCMSketch(0.0000001, 0.000001, 10 );
        PDDCMSketch pdd2 = new PDDCMSketch(0.0000001, 0.000001, 10 );

        PDDHistogram pddh = new PDDHistogram(10, 10);

        PDDSet set = new PDDSet(pdd1, pdd2, pddh );


        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput );

        // Operations on the data set go here
        MapOperator pddBuilder = MapOperator.builder(new PDDCarrier(set))
                .input(source)
                .name("local sketches")
                .build();

        pddBuilder.setDegreeOfParallelism(5);


        ReduceOperator pddCombiner = ReduceOperator.builder( new PDDCombiner() )
                .input(pddBuilder)
                .name("merge sketches")
                .build();


        FileDataSink sink = new FileDataSink( new CsvOutputFormat(), output, pddCombiner );

        CsvOutputFormat.configureRecordFormat(sink)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(StringValue.class, 0);


        return new Plan(sink);
    }

    @Override
    public String getDescription() {
        return null;
    }
}