package islab.keyplayer;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Map;

public class ValueComparator2 implements Comparator<String> {
	Map<String, BigDecimal> base;

    public ValueComparator2(Map<String, BigDecimal> base) {
        this.base = base;
    }

	@Override
	public int compare(String o1, String o2) {
		// TODO Auto-generated method stub
		BigDecimal i1 = base.get(o1);
		BigDecimal i2 = base.get(o2);
		return (i1.compareTo(i2) == -1) ? 1 : -1;
	}
}
