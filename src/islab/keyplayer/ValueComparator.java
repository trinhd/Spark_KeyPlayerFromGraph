package islab.keyplayer;

import java.util.Comparator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ValueComparator implements Comparator<JavaRDD<String>> {
	
	@Override
	public int compare(JavaRDD<String> o1, JavaRDD<String> o2) {
		// TODO Auto-generated method stub
		long l1 = o1.count();
		long l2 = o2.count();
		return l1 >= l2 ? -1 : 1;
	}
}