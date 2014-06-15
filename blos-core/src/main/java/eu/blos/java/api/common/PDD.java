package eu.blos.java.api.common;

import eu.stratosphere.types.Value;

import java.io.Serializable;

/**
 * Partitioned Distributed Dataset (PDD)
 * @param <T>
 */
public interface PDD<T extends PDD> extends /*Cloneable , Value*/ Serializable {

    /**
     * this method is called if this pdd was distributed to the nodes
     */
    public void alloc();

    /**
     * call in the reducer to merge the partitioned distributed dataset
     * @param s
     */
    public void mergeWith(T s);

    /**
     * no usage any more
     */
    //public T clone_mask();
    public void print();

}
