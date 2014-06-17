package eu.blos.java.algorithms.sketches;

import eu.blos.java.api.common.PDD;
import eu.blos.scala.algorithms.sketches.CMSketch;

public class PDDCMSketch extends CMSketch implements PDD<PDDCMSketch> {
    @Override
    public void mergeWith(PDDCMSketch s) {
        System.out.println("merge PDD Sketches");
        super.mergeWith( s );
    }
}