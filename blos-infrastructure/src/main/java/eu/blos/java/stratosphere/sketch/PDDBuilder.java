package eu.blos.java.stratosphere.sketch;

import eu.blos.java.api.common.PDDSet;
import eu.blos.java.api.common.Sketcher;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.io.SerializedOutputFormat;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;


public class PDDBuilder implements Program, ProgramDescription, Serializable {

    static final long serialVersionUID = 1L;

    /**
     * context for the PDD (e.g. hash functions) for local allocation
     * very important to allow a merging phase after the skeching phase
     */
    private PDDSet set;

    /**
     * actual code that do the skeching
     */
    private Sketcher sketcher;


    public PDDBuilder(PDDSet set, Sketcher sketcher ){
        this.set = set;
        this.sketcher = sketcher;
    }


    /**
     * construct the partial PDD during a mapping phase
     */

    public static class PDDCarrier extends MapFunction implements Serializable {

        public Collector<Record> collector = null;

        public static PDDSet set;

        public static Sketcher sketcher;

        public static int ActiveMapper = 0;

        public PDDCarrier(PDDSet set, Sketcher sketcher){
            this.set = set;
            this.sketcher = sketcher;

            // only allocate the set once one the machine
            set.alloc();
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ActiveMapper++;
        }

        public void close() throws Exception {
            ActiveMapper--;

            if(ActiveMapper == 0 ) {
                Record r = new Record();
                //r.setField(0, set);
                collector.collect(r);
            }

            super.close();
        }


        public void map(Record record, Collector<Record> out) {
            if(collector==null) collector = out;

            sketcher.update(set, record );
        }
    }

    /**
     * merge partial sketches into one
     */
    public static class PDDCombiner extends ReduceFunction implements Serializable {

        public PDDCombiner(){
        }

        public void reduce( Iterator<Record> records, Collector<Record> out) throws Exception {
            PDDSet global_PDD = null;
            Record element = null;

            while (records.hasNext()) {

                element = records.next();

                PDDSet pddset = PDDSet.class.newInstance();
                //element.getFieldInto(0, pddset);

                // prepare global PDD
                if (global_PDD == null) {
                    global_PDD = pddset;
                } else {
                    global_PDD.mergeWith(pddset);
                }
            }

            //global_PDD.print();

            //out.collect(new Record(global_PDD));
        }
    }


    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 0 ? args[0] : "");
        String output    = (args.length > 1 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput );

        // Operations on the data set go here
        MapOperator pddBuilder = MapOperator.builder(new PDDCarrier(set, sketcher ))
                .input(source)
                .name("local sketches")
                .build();

        pddBuilder.setDegreeOfParallelism(2);


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