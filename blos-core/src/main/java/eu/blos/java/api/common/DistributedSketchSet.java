package eu.blos.java.api.common;

import java.util.HashMap;
import java.util.Map;

public class DistributedSketchSet implements DistributedSketch {

    //private SketchSet sketchset = null; //new SketchSet();
    public static SketchSet sketchset_mask = null; //new SketchSet();

    public DistributedSketchSet( SketchSet set ) {
        this.sketchset_mask = set;
    }

    @Override
    public Sketch new_partial_sketch() {
        return sketchset_mask.clone_mask();
    }
}
