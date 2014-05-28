package eu.blos.java.api.common;

import java.util.HashMap;
import java.util.Map;

public class DistributedSketchSet implements DistributedSketch {
    private Map<String, Sketch> sketches = new HashMap<String, Sketch>();

    public DistributedSketchSet() {
    }

    @Override
    public Sketch new_partial_sketch() {
        return null;
    }
}
