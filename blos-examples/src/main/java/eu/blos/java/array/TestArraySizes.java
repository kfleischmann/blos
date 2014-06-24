package eu.blos.java.array;

import eu.blos.java.algorithms.sketches.PDDCMSketch;
import eu.blos.java.api.common.PDDSet;
import pl.edu.icm.jlargearrays.FloatLargeArray;

public class TestArraySizes {
    public static void main(String[] args) throws Exception {
        System.out.println("Test Memory");
        PDDCMSketch pdd1 = new PDDCMSketch(0.01, 0.00000001);

        pdd1.alloc();

        pdd1.update("hallo", 1 );
        pdd1.update("hallo", 1 );
        pdd1.update("hallo", 1 );


        System.out.println( pdd1.get("hallo") );

    }
}
