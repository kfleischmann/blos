package eu.blos.java.api.common;

import eu.stratosphere.types.Value;

public interface Sketch extends Value {
    public void alloc();
}