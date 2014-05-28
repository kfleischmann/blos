package eu.blos.java.api.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * organize multiple sketches
 */
public class SketchSet implements Sketch {

    @Override
    public void alloc() {

    }

    @Override
    public void mergeWith(Sketch s) {
        SketchSet set = (SketchSet)s;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void read(DataInput dataInput) throws IOException {

    }
}
