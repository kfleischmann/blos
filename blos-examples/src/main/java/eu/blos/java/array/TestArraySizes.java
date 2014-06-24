package eu.blos.java.array;

import eu.blos.java.algorithms.sketches.PDDCMSketch;
import eu.blos.java.api.common.PDDSet;
import pl.edu.icm.jlargearrays.FloatLargeArray;

public class TestArraySizes {
    public static void main(String[] args) throws Exception {
        System.out.println("Test Memory");
        int length=500;

        //byte[] buffer = new byte[size*1024*1024];
        //int[] buffer = new int[size*1024*1024];
        //byte[][] count = new byte[(length*(1024*1024)/1)][1];
        //byte[][] count = new byte [12][(length*(1024*1024)/1)];

        PDDCMSketch pdd1 = new PDDCMSketch(0.01, 0.00000001);

        pdd1.alloc();
        //System.out.println( pdd1.memory_size() / 1024.0 / 1024.0 );
        //PDDCMSketch pdd2 = new PDDCMSketch(0.1, 0.1);
        //PDDSet set = new PDDSet(pdd1);
        //set.alloc();
        /*
        long w = 1684354560L;
        long d = 1;

        FloatLargeArray a = new FloatLargeArray(w*d);*/


        pdd1.update("hallo", 1 );
        pdd1.update("hallo", 1 );
        pdd1.update("hallo", 1 );


        System.out.println( pdd1.get("hallo") );

    }
}
