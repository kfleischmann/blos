package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.FieldNormalizer;
import eu.blos.java.algorithms.sketches.field_normalizer.RoundNormalizer;
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

	//public static FieldNormalizer normalizer = new ZeroOneNormalizer(10);
	public static FieldNormalizer normalizer =  new RoundNormalizer(6);

	public static void main(String[] args)  {

		File file = new File("/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/datasets/linear_regression/dataset12");

		long lines=0;
		try (BufferedReader br = new BufferedReader( new FileReader( file ) )) {
			String line;

			while ((line = br.readLine()) != null) {
				String[] values =line.split(",");

				if(lines%100000 == 0)
				System.out.println("read lines "+lines);


				Tuple1<Double> Yi = new Tuple1<Double>( Double.parseDouble(values[1]) );
				Tuple2<Double,Double> Xi = new Tuple2<>( 1.0, Double.parseDouble(values[2]) );

				dataset.add(Xi);
				labels.add(Yi);
				lines++;
				}
			} catch (IOException e) {
				e.printStackTrace();
		}


		double total_size=0.0;
		sketch1.add( new CMSketch(0.03, 0.0001 ) );
		sketch1.add( new CMSketch(0.03, 0.0001 ) );

		sketch2.add( new CMSketch(0.03, 0.0001 ) );
		sketch2.add( new CMSketch(0.03, 0.0001 ) );
		sketch2.add( new CMSketch(0.03, 0.0001 ) );
		sketch2.add( new CMSketch(0.03, 0.0001 ) );


		for( CMSketch s : sketch1 ){
			s.alloc();
			System.out.println(s.w());
			System.out.println(s.d());

			total_size += s.alloc_size();
		}

		for( CMSketch s : sketch2 ){
			s.alloc();
			System.out.println(s.w());
			System.out.println(s.d());

			total_size += s.alloc_size();
		}

		System.out.println("total hash-size: "+ (total_size/1024.0/1024.0 )+"mb");

		System.exit(0);

		double max=0.0;
		double min=0.0;

		String lookup;
		for(int k=0; k < 2; k++ ) {
			System.out.println("k:"+k);
			for (int i = 0; i < dataset.size(); i++) {
				double yi_xik0 = (double) labels.get(i).getField(0) * (double) dataset.get(i).getField(k);
				lookup = ""+normalizer.normalize(yi_xik0);
				//System.out.println(yi_xik0+" => " + lookup );

				sketch1.get(k).update(lookup);
				for (int j = 0; j < 2; j++) {
					double xij_xik0 = (double) dataset.get(i).getField(j) * (double) dataset.get(i).getField(k);

					if( i%1000 == 0)
						System.out.println(k+" "+(i)+" "+j);

					max = Math.max( max, xij_xik0);
					min = Math.min( min, xij_xik0);


					lookup=""+normalizer.normalize(xij_xik0);
					sketch2.get(k*2+j).update(lookup);

					//System.out.println(+xij_xik0+" => " + lookup + " "+ sketch2.get(k*2+j).get(lookup));

				}//for
			}//for
		}

		System.out.println("max:"+max);
		System.out.println("min:"+min);

		for( CMSketch s : sketch1 ){
			System.out.println("");
			//s.display();
		}//for

		for( CMSketch s : sketch2 ){
			System.out.println("");
			//s.display();
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
		Double alpha=0.5;
		Double[] theta = {0.3, 0.3};
		Double[] theta_old = {0.3, 0.3};

		for( int i=0; i < 500; i++ ) {
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

		// real gradient decent
		//for( int i=0; i < dataset.size(); i++ ) {
			//result+= - labels.get(i).f0 * (double)dataset.get(i).getField(k) / (double)dataset.size();
			//}//for

		sum2 += -sketchEstimate( sketch1.get(k) , normalizer )  / (double)dataset.size();

		System.out.println( "real: "+result+" --- estimate "+sum2 );

		for( int j=0; j < d; j++ ) {
			// real gradient decent
			//for (int i = 0; i < dataset.size(); i++) {
				//result += theta[j] * (double)dataset.get(i).getField(j) * (double)dataset.get(i).getField(k) / (double)dataset.size();
			//}//for
			sum2 += theta[j] * sketchEstimate( sketch2.get(k*2+j) , normalizer ) / (double)dataset.size();

			System.out.println( "real: "+result+" --- estimate "+sum2 );

		}//for

		return sum2;
		//return result; // result of real gradient decent
	}

	public static Double sketchEstimate(CMSketch sketch, FieldNormalizer normalizer ){
		double sum = 0.0;
		long counter = sketch.totalSumPerHash();
		long freq;
		String lookup;
		for(double l=(double)normalizer.getMin(); l < (double)normalizer.getMax(); l+=(double)normalizer.getStep() ){
			//if(freq>0)System.out.println("lookup: "+lookup+" "+freq);
			lookup = ""+normalizer.normalize(l);
			freq =  sketch.get(""+lookup);
			sum += l * freq;
			counter += freq;
		}//for
		return sum;
	}
}
