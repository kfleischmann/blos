package eu.blos.java.api.common;

import java.util.HashMap;
import java.util.Map;

public class DistributedSketchSet implements DistributedSketch {
    private SketchSet sketchset = null; //new SketchSet();

    public DistributedSketchSet( SketchSet set ) {
        this.sketchset = set;
    }

    @Override
    public Sketch new_partial_sketch() {
        return sketchset.clone_mask();
    }
}
