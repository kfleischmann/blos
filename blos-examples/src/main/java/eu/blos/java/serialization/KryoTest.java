package eu.blos.java.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.blos.java.api.common.PDDSet;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.blos.java.algorithms.sketches.PDDCMSketch;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class KryoTest {
    public static void main(String[] argrs ) throws Exception {

        PDDCMSketch pdd1 = new PDDCMSketch(0.1, 0.1);
        PDDCMSketch pdd2 = new PDDCMSketch(0.1, 0.1);

        PDDSet set = new PDDSet(pdd1, pdd2 );

        set.alloc();



        System.out.println( pdd1.get_hashfunctions().size() );
        pdd1.update("test", 1 );
        pdd1.print();


        //PDDCMSketch pdd2 = pdd.clone();
        //pdd2.alloc();

        Kryo kryo = new Kryo();

        // ...
        Output output = new Output(new FileOutputStream("/home/kay/file.bin"));

        kryo.writeClassAndObject(output, set);
        output.close();

        System.out.println("total:"+output.total() );

        Input input = new Input(new FileInputStream("/home/kay/file.bin"));

        Object object = kryo.readClassAndObject(input);

        if (object instanceof PDDSet) {
            System.out.println("joo");
            PDDSet s = (PDDSet)object;
            //s.print();
        }
        input.close();
    }
}
