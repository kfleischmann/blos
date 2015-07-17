package eu.blos.java.algorithms.sketches;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import eu.blos.scala.algorithms.sketches.CMSketch;
import org.junit.Test;


public class CMSketchTest {
	@Test
	public void testSingleIncrement (){


		CMSketch sketch = new CMSketch(0.05, 0.0005 );

		sketch.alloc();

		sketch.update("test");
		sketch.update("test");
		sketch.update("test");

		sketch.update("test1");

		assert( sketch.get("test") ==  3);
		assert( sketch.get("test1") ==  1);
	}
}
