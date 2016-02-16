package islab.keyplayer;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

public class ValueComparator2 implements Comparator<BigDecimal> {
	
	@Override
	public int compare(BigDecimal o1, BigDecimal o2) {
		// TODO Auto-generated method stub
		return (o1.compareTo(o2));
	}
}
