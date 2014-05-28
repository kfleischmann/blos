package eu.blos.java.stratosphere.sketch;

import eu.blos.java.api.common.Sketch;
import eu.blos.scala.algorithms.sketches.CMSketch;
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
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import eu.blos.java.api.common.DistributedSketcher;
import eu.blos.scala.algorithms.sketches.DistributedCMSketcher;


public class SketchBuilder implements Program, ProgramDescription, Serializable {

    public static DistributedSketcher distributedSketcher = new DistributedCMSketcher(0.1, 0.1, 10 );

    private Sketcher sketcher = null;

    public SketchBuilder( Sketcher sketcher ){
        this.sketcher = sketcher;
    }

    public interface Sketcher extends Serializable {
        public void update( Sketch s, Record tuple );
    }

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
            CMSketch sketch = record.getField(0, CMSketch.class);
            sketch.write( dataOutputStream ) ;
        }

        @Override
        public void close() throws IOException {
            super.close();
        }
    }


    public static class PartialSketch extends MapFunction implements Serializable {

        private Sketch sketch = null;

        private Collector<Record> collector = null;

        private Sketcher sketcher = null ;

        public PartialSketch( Sketcher sketcher ){
            this.sketcher = sketcher;
        }

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            sketch = distributedSketcher.new_partial_sketch();

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


    public static class MergeSketch extends ReduceFunction implements Serializable {

        public void reduce( Iterator<Record> records, Collector<Record> out) {
            Record element = null;
            CMSketch global_sketch = null;
            if (records.hasNext()) {
                global_sketch = records.next().getField(0, CMSketch.class);

                while (records.hasNext()) {
                    element = records.next();
                    CMSketch sketch = element.getField(0, CMSketch.class);
                    global_sketch.mergeWith(sketch);
                }//while
            }//if

            out.collect( new Record( global_sketch ) );
        }
    }

    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 0 ? args[0] : "");
        String output    = (args.length > 1 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput );

        // Operations on the data set go here

        // ...
        MapOperator sketcher = MapOperator.builder(new PartialSketch( this.sketcher ))
                .input(source)
                .name("local sketches")
                .build();

        ReduceOperator merger = ReduceOperator.builder( MergeSketch.class )
                .input(sketcher)
                .name("merge sketches")
                .build();


        FileDataSink sink = new FileDataSink( new SketchOutputFormat(), output, merger );

        return new Plan(sink);
    }


    public static void main(String[] args) throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        LocalExecutor executor = new LocalExecutor();
        executor.start();


        executor.executePlan( new SketchBuilder( new Sketcher(){
            @Override
            public void update(Sketch s, Record tuple) {

                /* do the sketching here */
                String[] line = tuple.getField(0, StringValue.class).getValue().split(" ");


                CMSketch cmSketch = (CMSketch)s;

                cmSketch.update("hallo", 123 );
            }
        }).getPlan(inputPath, outputPath) );

        executor.stop();
    }

    @Override
    public String getDescription() {
        return null;
    }
}