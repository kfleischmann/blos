package eu.blos.java.api.common;

import java.util.ArrayList;
import java.util.List;

/**
 * organize multiple PDDs (Partitioned Distributed Dataset's)
 */
public class PDDSet implements PDD {
    private List<PDD> PDDs = new ArrayList<PDD>();

    private boolean allocated = false;

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

    public boolean isAllocated(){ return allocated; }

    @Override
    public void alloc() {
        if(!allocated) {
            for (PDD s : this.PDDs) {
                s.alloc();
            }//for
            allocated=true;
        }
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
}
