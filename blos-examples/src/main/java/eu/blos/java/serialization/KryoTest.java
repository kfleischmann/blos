package eu.blos.java.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class KryoTest {
    public static void main(String[] argrs ) throws Exception {


        //PDDCMSketch pdd2 = pdd.clone();
        //pdd2.alloc();

        Kryo kryo = new Kryo();

        // ...
        Output output = new Output(new FileOutputStream("/home/kay/file.bin"));

        kryo.writeClassAndObject(output, new String("test"));
        output.close();

        System.out.println("total:"+output.total() );

        Input input = new Input(new FileInputStream("/home/kay/file.bin"));

        Object object = kryo.readClassAndObject(input);

        if (object instanceof String) {
            System.out.println("joo");
        }
        input.close();
    }
}
