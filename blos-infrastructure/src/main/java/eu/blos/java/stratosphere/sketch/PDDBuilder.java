package eu.blos.java.stratosphere.sketch;

import eu.blos.java.api.common.PDD;
import eu.blos.java.api.common.Sketcher;
import eu.blos.scala.algorithms.sketches.PDDCMSketch;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.Record;
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

    public static class PartialPDD extends MapFunction implements Serializable {

        private Collector<Record> collector = null;

        private PDDCMSketch cmsketch = new PDDCMSketch(0.001, 0.001, 2);

        public PartialPDD(){
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            System.out.println("open mapper");

            // allocate
        }

        public void close() throws Exception {
            Record r = new Record();

            //r.setField();

            Value v;
            //r.write();

            collector.collect(r);

            super.close();
        }


        public void map(Record record, Collector<Record> out) {
            if(collector==null) collector = out;

            // execute
        }
    }

    /**
     * merge partial sketches into one
     */

    /*
    public static class MergeSketch extends ReduceFunction implements Serializable {
        private Class<? extends PDD> sketchType;

        public MergeSketch(Class<? extends PDD> type){
            this.sketchType = type;
        }

        public void reduce( Iterator<Record> records, Collector<Record> out) throws Exception {
            PDD global_PDD = null;
            Record element = null;
            while (records.hasNext()) {
                element = records.next();
                PDD PDD = (PDD)sketchType.newInstance();
                System.out.println("hascode: new instance "+ PDD.hashCode() );

                DataOutputView d
                element.serialize()

                element.getFieldInto(0, PDD);

                // prepare global PDD
                if(global_PDD == null ) {
                    global_PDD = PDD.clone_mask();
                    global_PDD.alloc();
                }

                //PDD.print();

                global_PDD.mergeWith(PDD);

            }

            //global_PDD.print();

            out.collect( new Record(global_PDD) );
    }*/


    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 0 ? args[0] : "");
        String output    = (args.length > 1 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput );

        // Operations on the data set go here
        MapOperator sketcher = MapOperator.builder(new PartialPDD())
                .input(source)
                .name("local sketches")
                .build();

        /*

        sketcher.setDegreeOfParallelism(3);

        ReduceOperator merger = ReduceOperator.builder( new MergeSketch(sketchType) )
                .input(sketcher)
                .name("merge sketches")
                .build();


        FileDataSink sink = new FileDataSink( new SketchOutputFormat(), output, merger );

        return new Plan(sink);*/
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}