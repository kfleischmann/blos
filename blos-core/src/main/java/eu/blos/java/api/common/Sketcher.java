package eu.blos.java.api.common;

import java.io.Serializable;

public interface Sketcher<T> extends Serializable {
    public void update( Sketch s, T tuple );
}