package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.PDDCMSketch;
import eu.blos.java.api.common.PDDSet;
import eu.blos.java.api.common.Sketcher;
import eu.blos.java.stratosphere.sketch.PDDBuilder;
import eu.blos.java.stratosphere.sketch.PDDReader;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;


public class RFLearning {

    public static void main(String[] args) throws Exception {

        String inputPath = "file:///home/kay/output";
        String outputPath=  "file:///home/kay/output2";

        LocalExecutor executor = new LocalExecutor();
        executor.start();

        Plan p = new PDDReader().getPlan(inputPath, outputPath );

        executor.executePlan( p );


    }

}
