package eu.blos.java.algorithms.sketches;


import eu.blos.scala.algorithms.sketches.CMSketch;
import org.junit.Test;


public class CMSketchTest {
	@Test
	public void testSingleIncrement (){
		CMSketch sketch = new CMSketch(0.01, 0.1 );
		sketch.alloc();

		sketch.update("test");
		sketch.update("test");
		sketch.update("test");
		sketch.update("test1");

		assert( sketch.get("test") ==  3);
		assert( sketch.get("test1") ==  1);
	}


	@Test
	public void testUniformHashFunction (){
		CMSketch sketch = new CMSketch(0.01, 0.1 );
		sketch.alloc();

		HashFunction[] hashfunctions = sketch.getHashfunctions();

		for( int i=0; i < 10000000; i++ ){
			sketch.update(""+i);
		}

		double epsilon = 0.008;
		double avgdiff = 0.0;
		double maxdiff = 0L;
		double currdiff = 0.0;
		long countdiff = 0L;

		for( long _row=0; _row < hashfunctions.length; _row ++ )
			for( long _col=0; _col < sketch.w(); _col++ )
				for( long row=0; row < hashfunctions.length; row ++ )
					for( long col=0; col < sketch.w(); col++ ) {
						currdiff = Math.abs(sketch.array_get(_row, _col) - sketch.array_get(row, col)) / (double)Math.abs(sketch.array_get(_row, _col));
						maxdiff = (double)Math.max(maxdiff, currdiff);
						avgdiff += maxdiff;
						countdiff += 1;
						assert (currdiff < epsilon);
					}
		avgdiff /= countdiff;

		assert( avgdiff < epsilon );
	}//for
}