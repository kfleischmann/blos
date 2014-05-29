package eu.blos.java.api.common;

import eu.stratosphere.types.Value;

public interface Sketch extends Value {
    public void alloc();
    public void mergeWith(Sketch s);
    public Sketch clone_mask();
    public void print();
}
