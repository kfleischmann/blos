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
        hashfunctions_$eq(generate_hashfunctions());
    }


    @Override
    public void mergeWith(PDDCMSketch s) {
        super.mergeWith( s );
    }

    @Override
    public long memory_size() {
        return this.w() * 4;
        //return this.count().length * this.count()[0].length*4;
    }
}