package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.ZeroOneNormalizer;
import eu.blos.java.flink.sketch.api.SketchBuilder;
import eu.blos.scala.algorithms.sketches.CMSketch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Test {

	public static List<Tuple2<Double,Double>> dataset = new ArrayList<Tuple2<Double,Double>>();

	public static List<Tuple1<Double>> labels = new ArrayList<Tuple1<Double>>();
	public static List<CMSketch> sketch1 = new ArrayList<CMSketch>();
	public static List<CMSketch> sketch2 = new ArrayList<CMSketch>();

	public static FieldNormalizer normalizer = new ZeroOneNormalizer(10);


	public static void main(String[] args)  {

		File file = new File("/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/datasets/linear_regression/dataset3");


		try (BufferedReader br = new BufferedReader( new FileReader( file ) )) {
			String line;

			while ((line = br.readLine()) != null) {
				String[] values =line.split(",");


				Tuple1<Double> Yi = new Tuple1<Double>( Double.parseDouble(values[1]) );

				Tuple2<Double,Double> Xi = new Tuple2<>( 1.0, Double.parseDouble(values[2]) );

				dataset.add(Xi);
				labels.add(Yi);
				}
			} catch (IOException e) {
				e.printStackTrace();
		}


		sketch1.add( new CMSketch(0.01, 0.008 ) );
		sketch1.add( new CMSketch(0.01, 0.008 ) );

		sketch2.add( new CMSketch(0.01, 0.008 ) );
		sketch2.add( new CMSketch(0.01, 0.008 ) );
		sketch2.add( new CMSketch(0.01, 0.008) );
		sketch2.add( new CMSketch(0.01, 0.008 ) );

		for( CMSketch s : sketch1 ){
			s.alloc();
			System.out.println(s.w());
		}

		for( CMSketch s : sketch2 ){
			s.alloc();
			System.out.println(s.w());
		}

		for(int k=0; k < 2; k++ ) {

			for (int i = 0; i < dataset.size(); i++) {
				double yi_xik0 = (double) labels.get(i).getField(0) * (double) dataset.get(i).getField(k);

				sketch1.get(k).update("" + normalizer.normalize(yi_xik0));

				for (int j = 0; j < 2; j++) {

					double xij_xik0 = (double) dataset.get(i).getField(j) * (double) dataset.get(i).getField(k);
					sketch2.get(k*2+j).update("" + normalizer.normalize(xij_xik0));
				}//for
			}//for
		}


		for( CMSketch s : sketch1 ){
			System.out.println("");
			s.display();
		}//for

		for( CMSketch s : sketch2 ){
			System.out.println("");
			s.display();
		}//for

		learn();


	}


	// real model: -0.14164507397556597 1.0434789479935427
	public static List<Tuple2<Double,Double[]>> testLinRegDataSet(){
		List<Tuple2<Double,Double[]>> dataset = new ArrayList<Tuple2<Double,Double[]>>();
		dataset.add(new Tuple2(-0.955629435186,  new Double[] {1.0, -0.75121113185}) );
		dataset.add(new Tuple2(0.490889720885,  new Double[] {1.0, 0.585311356523})) ;
		dataset.add(new Tuple2(-1.07238545278,  new Double[] {1.0, -0.925939426578}));
		dataset.add(new Tuple2(-0.390171914177,  new Double[] {1.0, -0.272969938626}));
		dataset.add(new Tuple2(0.782689711998,  new Double[] {1.0, 0.828812491524} ));
		dataset.add(new Tuple2(0.637338224205,  new Double[] {1.0, 0.78592062834} ));
		dataset.add(new Tuple2(-0.227083652156,  new Double[] {1.0, -0.0966025660222} ));
		dataset.add(new Tuple2(0.309557927922,  new Double[] {1.0, 0.4713667385} ));
		dataset.add(new Tuple2(-0.38246690061,  new Double[] {1.0, -0.229493696328} ));
		dataset.add(new Tuple2(-0.399638414267, new Double[] {1.0, -0.194375304678} ));
		return dataset;
	}

	public static void learn() {

		// m*x+b
		Double alpha=0.05;
		Double[] theta = {0.0, 0.0};
		Double[] theta_old = {0.0, 0.0};

		for( int i=0; i < 200; i++ ) {
			theta[0] = theta_old[0] - alpha*nextStep(0, theta_old);
			theta[1] = theta_old[1] - alpha*nextStep(1, theta_old);

			theta_old[0] = theta[0];
			theta_old[1] = theta[1];

			System.out.println( theta[0] + " "+theta[1]);
		}

	}

	public static List<Tuple2<Double,Double[] >> dataset2 = testLinRegDataSet();

	public static Double nextStep( int k, Double[] theta ){
		int d = theta.length;
		Double sum=0.0;
		Double result=0.0;

		Double sum2 = 0.0;

		for( int i=0; i < dataset.size(); i++ ) {
			result+= - labels.get(i).f0 * (double)dataset.get(i).getField(k);
		}//for

		sum2 += sketchEstimate( sketch1.get(k) , normalizer );

		for( int j=0; j < d; j++ ) {
			for (int i = 0; i < dataset.size(); i++) {
				result += theta[j] * (double)dataset.get(i).getField(j) * (double)dataset.get(i).getField(k);
			}//for

			sum2 -= sketchEstimate( sketch2.get(k*2+j) , normalizer );

		}//for
		return sum2;
		//return result;
	}




	public static Double sketchEstimate(CMSketch sketch, FieldNormalizer normalizer ){
		double sum = 0.0;
		long counter = sketch.totalSumPerHash();
		long freq;
		double a;

		for(int l=normalizer.getMin(); l < normalizer.getMax(); l++ ){
			a = normalizer.denormalize(l);
			freq =  sketch.get(""+l);
			sum += a * freq;
			counter += freq;
		}//for

		return  sum / (double)counter;
	}
}
