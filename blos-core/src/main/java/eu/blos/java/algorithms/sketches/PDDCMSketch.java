package eu.blos.java.algorithms.sketches;

import eu.blos.java.api.common.PDD;
import eu.blos.scala.algorithms.sketches.CMSketch;

public class PDDCMSketch extends CMSketch implements PDD<PDDCMSketch> {
    public PDDCMSketch(){

    }
    public PDDCMSketch( double delta, double epsilon ){
        super();
        delta_$eq(delta);
        epsilon_$eq(epsilon);
    }

    @Override
    public void mergeWith(PDDCMSketch s) {
        super.mergeWith( s );
    }
}