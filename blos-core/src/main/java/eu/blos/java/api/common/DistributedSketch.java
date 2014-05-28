package eu.blos.java.api.common;

import java.io.Serializable;

public interface DistributedSketch extends Serializable {
    public Sketch new_partial_sketch();
}
