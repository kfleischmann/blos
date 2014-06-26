package eu.blos.java.ml.random_forest;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;


public class RFLearning {

    public static void main(String[] args) throws Exception {

        String inputPath = "file:///home/kay/output";
        String outputPath=  "file:///home/kay/output2";

        LocalExecutor executor = new LocalExecutor();
        executor.start();

		/*
        Plan p = new PDDReader().getPlan(inputPath, outputPath );
        executor.executePlan( p );

        */

    }

}
