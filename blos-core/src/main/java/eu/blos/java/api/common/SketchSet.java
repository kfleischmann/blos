package eu.blos.java.api.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * organize multiple sketches
 */
public class SketchSet implements Sketch {
    private List<Sketch> sketches = new ArrayList<Sketch>();

    public SketchSet(){
        // no information about next sketch types
        // i have to take it implcitly
        sketches=DistributedSketchSet.sketchset_mask.getSketches();
    }

    public SketchSet( Sketch ... sketches ){
        for( Sketch s : sketches ){
            this.sketches.add(s.clone_mask());
        }
    }

    public SketchSet( List<Sketch> sketches ){
        for( Sketch s : sketches ){
            this.sketches.add(s.clone_mask());
        }
    }


    public List<Sketch> getSketches(){
        return sketches;
    }

    @Override
    public void alloc() {
        for( Sketch s : this.sketches ){
            s.alloc();
        }//for
    }

    @Override
    public void mergeWith(Sketch s) {
        SketchSet set = (SketchSet)s;
        assert set.getSketches().size() == getSketches().size();

        for(int i=0; i < set.getSketches().size(); i++ ){
            getSketches().get(i).mergeWith( set.getSketches().get(i) );
        }//for
    }

    @Override
    public Sketch clone_mask() {
        SketchSet set = new SketchSet( getSketches() );
        return set;
    }

    @Override
    public void print() {
        for(int i=0; i < getSketches().size(); i++ ){
            getSketches().get(i).print();
        }//for
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i=0; i < getSketches().size(); i++ ) {
            getSketches().get(i).write(dataOutput);
        }//for
    }

    @Override
    public void read(DataInput dataInput) throws IOException {
        for(int i=0; i < getSketches().size(); i++ ) {
            getSketches().get(i).read(dataInput);
        }//for
    }
}
