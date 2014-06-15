package eu.blos.java.api.common;

import java.io.Serializable;

public interface Sketcher<T> extends Serializable {
    public void update( PDD s, T tuple );
}