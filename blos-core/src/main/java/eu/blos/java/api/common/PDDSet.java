package eu.blos.java.api.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * organize multiple PDDs (Partitioned Distributed Dataset's)
 *
 */
public class PDDSet implements PDD {
    private List<PDD> PDDs = new ArrayList<PDD>();

    public PDDSet(){
    }

    public PDDSet(PDD... PDDs){
        for( PDD s : PDDs){
            this.PDDs.add(s);
        }
    }

    public PDDSet(List<PDD> PDDs){
        for( PDD s : PDDs){
            this.PDDs.add(s );
        }
    }

    public List<PDD> getPDDs(){
        return PDDs;
    }

    @Override
    public void alloc() {
        for( PDD s : this.PDDs){
            s.alloc();
        }//for
    }

    @Override
    public void mergeWith(PDD s) {
        PDDSet set = (PDDSet)s;
        assert set.getPDDs().size() == getPDDs().size();

        for(int i=0; i < set.getPDDs().size(); i++ ){
            getPDDs().get(i).mergeWith( set.getPDDs().get(i) );
        }//for
    }

    @Override
    public void print() {
        for(int i=0; i < getPDDs().size(); i++ ){
            getPDDs().get(i).print();
        }//for
    }


    /*
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i=0; i < getPDDs().size(); i++ ) {
            //getPDDs().get(i).write(dataOutput);
        }//for
    }

    @Override
    public void read(DataInput dataInput) throws IOException {
        for(int i=0; i < getPDDs().size(); i++ ) {
            //getPDDs().get(i).read(dataInput);
        }//for
    }*/

}
