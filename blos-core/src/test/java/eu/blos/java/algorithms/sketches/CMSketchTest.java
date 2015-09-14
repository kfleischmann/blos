package eu.blos.java.algorithms.sketches;


import eu.blos.scala.algorithms.sketches.CMEstimate;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.blos.scala.algorithms.sketches.HeavyHitters;
import org.junit.Test;
import scala.collection.Iterator;
import scala.collection.mutable.HashMap;


public class CMSketchTest {

	@Test
	public void testSingleIncrement (){
		CMSketch sketch = new CMSketch(0.01, 0.1, 1 );
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
		CMSketch sketch = new CMSketch(0.01, 0.1, 1  );
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


	@Test
	public void testHeavyHitters(){
		CMSketch sketch = new CMSketch(0.01, 0.1, 7  );
		sketch.alloc();

		// count test1 -> 1
		// count test2 -> 3
		// count test3 -> 2
		sketch.update("test-1");
		sketch.update("test-2");
		sketch.update("test-3");
		sketch.update("test-4");
		sketch.update("test-5");
		sketch.update("test-6");
		sketch.update("test-7");
		sketch.update("test-8");


		sketch.update("test1");

		sketch.update("test2");
		sketch.update("test2");
		sketch.update("test2");

		sketch.update("test3");
		sketch.update("test3");

		sketch.update("test3");
		sketch.update("test3");

		sketch.update("test1");
		sketch.update("test1");
		sketch.update("test1");
		sketch.update("test1");

		sketch.update("test4");
		sketch.update("test4");
		sketch.update("test4");
		sketch.update("test4");
		sketch.update("test4");

		sketch.update("test5");

		sketch.update("test6");

		sketch.update("test7");
		sketch.update("test7");
		sketch.update("test7");
		sketch.update("test7");


		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");
		sketch.update("test8");

		sketch.update("test9");
		sketch.update("test10");
		sketch.update("test11");
		sketch.update("test12");
		sketch.update("test13");
		sketch.update("test14");
		sketch.update("test16");
		sketch.update("test17");
		sketch.update("test18");

		HeavyHitters hh = sketch.getHeavyHitters();

		/*
		for( int i=0; i < hh.getHeapArray().length; i++ ){
			CMEstimate e = (CMEstimate)hh.getHeapArray()[i];
			System.out.println("hh: "+e);
		}*/

		assert( ((CMEstimate)hh.getHeapArray()[1]).count() == 10 && ((CMEstimate)hh.getHeapArray()[1]).key().equals("test8") );
		assert( ((CMEstimate)hh.getHeapArray()[2]).count() == 5 && ((CMEstimate)hh.getHeapArray()[2]).key().equals("test1") );
		assert( ((CMEstimate)hh.getHeapArray()[3]).count() == 5 && ((CMEstimate)hh.getHeapArray()[3]).key().equals("test4") );
		assert( ((CMEstimate)hh.getHeapArray()[4]).count() == 1 && ((CMEstimate)hh.getHeapArray()[4]).key().equals("test18") );
		assert( ((CMEstimate)hh.getHeapArray()[5]).count() == 4 && ((CMEstimate)hh.getHeapArray()[5]).key().equals("test7") );
		assert( ((CMEstimate)hh.getHeapArray()[6]).count() == 4 && ((CMEstimate)hh.getHeapArray()[6]).key().equals("test3") );
		assert( ((CMEstimate)hh.getHeapArray()[7]).count() == 3 && ((CMEstimate)hh.getHeapArray()[7]).key().equals("test2") );
	}
}