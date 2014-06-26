package eu.blos.java.api.common;

/**
 * Partitioned Distributed Dataset (PDD)
 * @param <T>
 */
public interface PDD<T extends PDD> {

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
     * receive the memory size needed for this sketch
     * @return
     */
    public long memory_size();

    /**
     * no usage any more
     */
    //public T clone_mask();
    public void print();

}