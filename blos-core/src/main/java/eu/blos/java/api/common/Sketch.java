package eu.blos.java.api.common;

import eu.stratosphere.types.Value;

public interface Sketch<S extends Sketch> extends Value {
    public void alloc();
    public void mergeWith(S s);
    public S clone_mask();
    public void print();
}
