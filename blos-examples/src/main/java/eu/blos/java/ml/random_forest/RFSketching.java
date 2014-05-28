package eu.blos.java.ml.random_forest;

import eu.blos.java.api.common.DistributedSketcher;
import eu.blos.java.api.common.Sketch;
import eu.blos.java.api.common.Sketcher;
import eu.blos.java.stratosphere.sketch.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.blos.scala.algorithms.sketches.DistributedCMSketcher;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class RFSketching {


    public static void main(String[] args) throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        DistributedSketcher distributedSketcher = new DistributedCMSketcher(0.1, 0.1, 10 );

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan( new SketchBuilder( CMSketch.class, new Sketcher<Record>(){
            @Override
            public void update(Sketch s, Record tuple) {
                /* do the sketching here */
                String[] line = tuple.getField(0, StringValue.class).getValue().split(" ");


                CMSketch cmSketch = (CMSketch)s;

                cmSketch.update("hallo", 123 );


            }}, distributedSketcher)
            .getPlan(inputPath, outputPath) );

        executor.stop();
    }
}
