package eu.blos.java.ml.random_forest;

import eu.blos.java.api.common.DistributedSketch;
import eu.blos.java.api.common.Sketch;
import eu.blos.java.api.common.Sketcher;
import eu.blos.java.stratosphere.sketch.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.blos.scala.algorithms.sketches.DistributedCMSketch;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class RFSketching {


    public static void main(String[] args) throws Exception {
        mutipleSketches();
    }

    public static void singleSketch() throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        DistributedSketch distributedSketch = new DistributedCMSketch(0.1, 0.1, 10 );

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan(new SketchBuilder(CMSketch.class, new Sketcher<Record>() {
            @Override
            public void update(Sketch s, Record tuple) {
                /* do the sketching here */
                String[] line = tuple.getField(0, StringValue.class).getValue().split(" ");


                CMSketch cmSketch = (CMSketch) s;

                for ( String l: line ) {
                    cmSketch.update( l, 1);
                }


            }
        }, distributedSketch)
                .getPlan(inputPath, outputPath));

        executor.stop();
    }


    public static void mutipleSketches() throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        DistributedSketch distributedSketch = new DistributedCMSketch(0.1, 0.1, 10 );

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan(new SketchBuilder(CMSketch.class, new Sketcher<Record>() {
            @Override
            public void update(Sketch s, Record tuple) {
                /* do the sketching here */
                String[] line = tuple.getField(0, StringValue.class).getValue().split(" ");


                CMSketch cmSketch = (CMSketch) s;

                for ( String l: line ) {
                    cmSketch.update( l, 1);
                }


            }
        }, distributedSketch)
                .getPlan(inputPath, outputPath));

        executor.stop();
    }

}
