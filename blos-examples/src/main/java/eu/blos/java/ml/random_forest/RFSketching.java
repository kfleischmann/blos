package eu.blos.java.ml.random_forest;

import eu.blos.java.api.common.PDDSet;
import eu.blos.java.api.common.Sketcher;
import eu.blos.java.stratosphere.sketch.PDDBuilder;
import eu.blos.scala.algorithms.PDDHistogram;
import eu.blos.java.algorithms.sketches.PDDCMSketch;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import java.lang.instrument.Instrumentation;


public class RFSketching {


    public static void main(String[] args) throws Exception {

        PDDCMSketch pdd1 = new PDDCMSketch(0.000001, 0.00004);
        PDDCMSketch pdd2 = new PDDCMSketch(0.000001, 0.00004);

        PDDSet set = new PDDSet();

        set.getPDDs().add(pdd1);
        set.getPDDs().add(pdd2);


        String inputPath = "file:///home/kay/normalized_small.txt";
        String outputPath=  "file:///home/kay/output";

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        Plan p = new PDDBuilder(set, new Sketcher<Record>() {
            @Override
            public void update(PDDSet set, Record tuple) {
                PDDCMSketch cm = (PDDCMSketch)set.getPDDs().get(0);

                String line = tuple.getField(0, StringValue.class ).getValue();
                String[] fields = line.split(" ");

                //cm.update( fields[0], 1);
            }
        }).getPlan(inputPath, outputPath );

        executor.executePlan( p );

    }

}
