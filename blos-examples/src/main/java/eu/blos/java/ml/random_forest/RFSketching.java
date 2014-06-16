package eu.blos.java.ml.random_forest;

import eu.blos.java.stratosphere.sketch.PDDBuilder;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;

public class RFSketching {


    public static void main(String[] args) throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        Plan p = new PDDBuilder().getPlan(inputPath, outputPath );

        executor.executePlan( p );


        //mutipleSketches();
    }

    /*
    public static void singleSketch() throws Exception {
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        DistributedSketch distributedSketch = new DistributedCMSketch(0.1, 0.1, 10 );

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan(new PDDBuilder(CMPDD.class, new Sketcher<Record>() {
            @Override
            public void update(PDD s, Record tuple) {

                String[] line = tuple.getField(0, StringValue.class).getValue().split(" ");


                CMPDD cmSketch = (CMPDD) s;

                for ( String l: line ) {
                    cmSketch.update( l, 1);
                }


            }
        }, distributedSketch)
                .getPlan(inputPath, outputPath));

        executor.stop();
    }*/


    public static void mutipleSketches() throws Exception {

        /*
        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        CMSketch cm1 = new CMSketch(0.1, 0.1, 10 );


        HistogramPDD dh = new HistogramPDD(10, 10 );

        DistributedSketch distributedSketch = new DistributedSketchSet( new PDDSet(cm1, dh) );
        //DistributedSketch distributedSketch = new DistributedSketchSet( new PDDSet(cm1, cm2, cm3, cm4, cm5) );

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        executor.executePlan(new PDDBuilder( PDDSet.class, new Sketcher<Record>() {
            @Override
            public void update(PDD s, Record tuple) {
                String[] line = tuple.getField(0, StringValue.class).getValue().split(" ");
                PDDSet set = (PDDSet)s;


                ((CMSketch)set.getPDDs().get(0)).update("lala", 1 );

                CMSketch cmSketch = (CMSketch) set.getPDDs().get(0);
                HistogramPDD histogramSketch = (HistogramPDD) set.getPDDs().get(1);

                histogramSketch.update(Double.parseDouble(line[1]));

            }
        }, distributedSketch) .getPlan(inputPath, outputPath));

        executor.stop();
        */
    }

}
