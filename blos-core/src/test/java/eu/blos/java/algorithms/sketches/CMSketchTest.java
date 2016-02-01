package eu.blos.java.algorithms.sketches;


import eu.blos.scala.algorithms.sketches.CMEstimate;
import eu.blos.scala.algorithms.sketches.CMSketch;
import eu.blos.scala.algorithms.sketches.HeavyHitters;
import org.junit.Test;
import scala.collection.Iterator;
import scala.collection.mutable.HashMap;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;
import java.util.Random;

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


	public int[][] generateTestDataset(int n, int N){
		int [][] data = new int [n][n];
		// default values shold be zero
		Random rng1 = new Random();
		Random rng2 = new Random();

		for (int k = 0; k < N; ++k) {
			double x = rng1.nextGaussian();
			double y = rng2.nextGaussian();

			int i = Math.abs((int)(((x+3)/5)*(float)n));
			int j = Math.abs((int)(((y+3)/5)*(float)n));

			if( i < n & j < n)
				data[i][j] += 1;
		}
		return data;
	}


	@Test
	/**
	 * Test sketch
	 *
	 * generate sketch
	 * elta=0.01 and epsilon=0.1
	 * N=1mio
	 * total sum is 1mio
	 *
	 */
	public void testSketch () {


		double delta = 0.01;        // certaintly
		double epsilon = 0.0001;        // error percentage

		CMSketch sketch = new CMSketch(delta, epsilon, 1);
		sketch.alloc();
		HashFunction[] hashfunctions = sketch.getHashfunctions();

		long 	N  	= 0;
		int		d 	= 1000;
		int[][] dataset = generateTestDataset( d, 10000000 );
		int		n 	= dataset.length*dataset[0].length;

		int real;

		// update sketch
		for (int i = 0; i < dataset.length; i++) {
			for (int j = 0; j < dataset[i].length; j++) {
				real = dataset[i][j];
				N += real;
				sketch.update(""+i+""+j, real );
			}
		}

		long errors = (long) (epsilon * (float) N);        // upper bound (2/w))*epsilon = #errors
		long errors2 = (2 * N / sketch.w());            // upper bound (2*N)/w = #errors

		assert (errors == errors2);

		long count = 0L;
		long bounded = 0L;
		long w = sketch.w();
		long total_errors = 0;

		sketch.print();

		for (int i = 0; i < dataset.length; i++) {
			for (int j = 0; j < dataset[i].length; j++) {
				count = sketch.get(""+i+""+j);
				real = dataset[i][j];
				// true value i=1
				// expected c <= real+(2*N/w)
				total_errors += Math.abs(real-count);
				if (count <= real + errors ) {
					bounded++;
				} // if
			} // for
		} // for
		// check certainty
		assert( (1.0-(float)bounded/(float)n) <= delta );
	}

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

		for( int i=0; i < hh.getHeapArray().length; i++ ){
			CMEstimate e = (CMEstimate)hh.getHeapArray()[i];
			System.out.println("hh: "+e);
		}

		assert( ((CMEstimate)hh.getHeapArray()[1]).count() == 10 && ((CMEstimate)hh.getHeapArray()[1]).key().equals("test8") );
		assert( ((CMEstimate)hh.getHeapArray()[2]).count() == 5 && ((CMEstimate)hh.getHeapArray()[2]).key().equals("test1") );
		assert( ((CMEstimate)hh.getHeapArray()[3]).count() == 5 && ((CMEstimate)hh.getHeapArray()[3]).key().equals("test4") );
		assert( ((CMEstimate)hh.getHeapArray()[4]).count() == 1 && ((CMEstimate)hh.getHeapArray()[4]).key().equals("test18") );
		assert( ((CMEstimate)hh.getHeapArray()[5]).count() == 4 && ((CMEstimate)hh.getHeapArray()[5]).key().equals("test7") );
		assert( ((CMEstimate)hh.getHeapArray()[6]).count() == 4 && ((CMEstimate)hh.getHeapArray()[6]).key().equals("test3") );
		assert( ((CMEstimate)hh.getHeapArray()[7]).count() == 3 && ((CMEstimate)hh.getHeapArray()[7]).key().equals("test2") );
	}
}