package eu.blos.java.stratosphere.sketch;

import eu.blos.scala.algorithms.sketches.CMSketch;
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
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import java.io.Serializable;
import java.util.Iterator;

import eu.blos.scala.algorithms.sketches.DistrubutedCMSketch;


public class SketchBuilder implements Program, ProgramDescription, Serializable {

    public static DistrubutedCMSketch distributedSketch = new DistrubutedCMSketch(0.1, 0.1, 10 );;

    public static class PartialSketch extends MapFunction implements Serializable {

        private CMSketch sketch = null;

        private Collector<Record> collector = null;

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
            sketch.update("hallo", 1 );
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

                    sketch.print();

                    System.out.println("merge");
                    global_sketch.mergeWith( sketch );
                }//while

                global_sketch.print();
            }//if

            out.collect( new Record( global_sketch ) );
        }
    }

    @Override
    public Plan getPlan(String... args) {

        String dataInput = (args.length > 1 ? args[0] : "");
        String output    = (args.length > 2 ? args[1] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput );

        // Operations on the data set go here
        // ...
        MapOperator sketcher = MapOperator.builder(new PartialSketch())
                .input(source)
                .name("local sketches")
                .build();

        sketcher.setDegreeOfParallelism(5);


        ReduceOperator merger = ReduceOperator.builder( MergeSketch.class )
                .input(sketcher)
                .name("merge sketches")
                .build();


        FileDataSink sink = new FileDataSink( new CsvOutputFormat(), output, merger );

        CsvOutputFormat.configureRecordFormat(sink)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(CMSketch.class, 0);

        return new Plan(sink);
    }


    public static void main(String[] args) throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

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