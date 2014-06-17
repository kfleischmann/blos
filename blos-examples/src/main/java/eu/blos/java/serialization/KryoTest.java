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

        PDDCMSketch pdd1 = new PDDCMSketch(0.001, 0.004);
        PDDCMSketch pdd2 = new PDDCMSketch(0.001, 0.004);

        PDDSet set = new PDDSet(pdd1, pdd2 );


        //PDDCMSketch pdd2 = pdd.clone();

        set.alloc();

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
