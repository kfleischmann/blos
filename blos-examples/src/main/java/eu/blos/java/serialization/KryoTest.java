package eu.blos.java.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.blos.java.api.common.PDDSet;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.blos.scala.algorithms.sketches.PDDCMSketch;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class KryoTest {
    public static void main(String[] argrs ) throws Exception {

        CMSketch pdd = new CMSketch(0.0000001, 0.000001, 10 );

        PDDCMSketch pdd1 = new PDDCMSketch(0.0000001, 0.000001, 10 );
        PDDCMSketch pdd2 = new PDDCMSketch(0.0000001, 0.000001, 10 );

        PDDSet set = new PDDSet(pdd1, pdd2 );


        //PDDCMSketch pdd2 = pdd.clone();

        set.alloc();

        //pdd2.alloc();

        Kryo kryo = new Kryo();

        // ...
        Output output = new Output(new FileOutputStream("/home/kay/file.bin"));

        kryo.writeClassAndObject(output, set);
        output.close();


        Input input = new Input(new FileInputStream("/home/kay/file.bin"));

        Object object = kryo.readClassAndObject(input);

        if (object instanceof PDDSet) {
            System.out.println("joo");
        }
        input.close();
    }
}
