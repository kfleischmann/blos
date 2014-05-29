package eu.blos.java.stratosphere.sketch;

import eu.blos.java.api.common.Sketch;
import eu.blos.java.api.common.Sketcher;
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
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import eu.blos.java.api.common.DistributedSketch;


public class SketchBuilder implements Program, ProgramDescription, Serializable {

    /**
     * context for the sketch (e.g. hash functions) for local allocation
     * very important to allow a merging phase after the skeching phase
     */
    public DistributedSketch distributedSketch = null ;

    /**
     * actual code that do the skeching
     */
    private Sketcher<Record> sketcher = null;

    private Class<? extends eu.stratosphere.types.Value> sketchType;

    public SketchBuilder(   Class<? extends eu.stratosphere.types.Value> type,
                            Sketcher<Record> sketcher,
                            DistributedSketch ds ){
        this.sketcher = sketcher;
        this.distributedSketch = ds;
        this.sketchType = type;
    }

    /**
     * write the sketch on disk
     */
    public class SketchOutputFormat extends FileOutputFormat<Record> {
        private static final long serialVersionUID = 1L;
        private DataOutputStream dataOutputStream;

        @Override
        public void open(int i) throws IOException {
            super.open(i);
            dataOutputStream = new DataOutputStream(stream);
        }

        @Override
        public void writeRecord(Record record) throws IOException {
            Sketch sketch = (Sketch)record.getField(0, sketchType);

            sketch.write( dataOutputStream ) ;
        }

        @Override
        public void close() throws IOException {
            super.close();
        }
    }

    /**
     * construct the partial sketch during a mapping phase
     */
    public static class PartialSketch extends MapFunction implements Serializable {

        private Sketch sketch = null;

        private Collector<Record> collector = null;

        private Sketcher sketcher = null ;

        private DistributedSketch distributedSketch = null;

        public PartialSketch( Sketcher sketcher, DistributedSketch ds ){
            this.sketcher = sketcher;
            this.distributedSketch = ds;
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            sketch = distributedSketch.new_partial_sketch();

            sketch.alloc();
        }

        public void close() throws Exception {
            Record r = new Record();

            r.setField(0, sketch);

            collector.collect(r);

            super.close();
        }

        public void map(Record record, Collector<Record> out) {
            if(collector==null) collector = out;
            sketcher.update( sketch, record );
        }
    }

    /**
     * merge partial sketches into one
     */
    public static class MergeSketch extends ReduceFunction implements Serializable {
        private Class<? extends eu.stratosphere.types.Value> sketchType;

        public MergeSketch(Class<? extends eu.stratosphere.types.Value> type){
            this.sketchType = type;
        }

        public void reduce( Iterator<Record> records, Collector<Record> out) {
            Record element = null;
            Sketch global_sketch = null;
            Sketch sketch = null;
            if (records.hasNext()) {
                 sketch = ((Sketch)records.next().getField(0, sketchType ));

                global_sketch = sketch.clone_mask();
                global_sketch.alloc();

                global_sketch.mergeWith( sketch );

                while (records.hasNext()) {
                    element = records.next();
                    sketch = (Sketch)element.getField(0, sketchType );
                    global_sketch.mergeWith(sketch);
                }//while
            }//if

            global_sketch.print();

            out.collect( new Record( global_sketch ) );
        }
    }

    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 0 ? args[0] : "");
        String output    = (args.length > 1 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput );

        // Operations on the data set go here
        MapOperator sketcher = MapOperator.builder(new PartialSketch( this.sketcher, distributedSketch))
                .input(source)
                .name("local sketches")
                .build();

        sketcher.setDegreeOfParallelism(10);

        ReduceOperator merger = ReduceOperator.builder( new MergeSketch(sketchType) )
                .input(sketcher)
                .name("merge sketches")
                .build();


        FileDataSink sink = new FileDataSink( new SketchOutputFormat(), output, merger );

        return new Plan(sink);
    }

    @Override
    public String getDescription() {
        return null;
    }
}