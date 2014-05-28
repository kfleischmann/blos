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

    public SketchSet( Sketch ... sketches ){
        for( Sketch s : sketches ){
            this.sketches.add(s);
        }
    }


    public List<Sketch> getSketches(){
        return sketches;
    }

    @Override
    public void alloc() {
        for( Sketch s : this.sketches ){
            s.alloc();
        }
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
        SketchSet set = new SketchSet();
        for(int i=0; i < getSketches().size(); i++ ){
            set.getSketches().add( getSketches().get(i).clone_mask()  );
        }
        return set;
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
