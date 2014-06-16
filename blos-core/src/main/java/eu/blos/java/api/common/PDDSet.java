package eu.blos.java.api.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import eu.blos.java.api.io.DataInputInputStream;
import eu.blos.java.api.io.DataOutputOutputStream;
import eu.stratosphere.types.Value;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * organize multiple PDDs (Partitioned Distributed Dataset's)
 *
 */
public class PDDSet implements PDD, Value {
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




    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Kryo kryo = new Kryo();
        OutputStream dout = DataOutputOutputStream.constructOutputStream(dataOutput);
        Output output = new Output(dout);
        kryo.writeClassAndObject(output, getPDDs() );
        output.close();
    }

    @Override
    public void read(DataInput dataInput) throws IOException {
        Kryo kryo = new Kryo();
        InputStream din = DataInputInputStream.constructInputStream(dataInput);
        Input input = new Input(din);
        Object o = kryo.readObject(input, getPDDs().getClass() );
        PDDs = (List<PDD>)o;
        input.close();
    }

}
