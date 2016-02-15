package islab.keyplayer;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ValueComparator implements Comparator<String> {
    Map<String, List<String>> base;

    public ValueComparator(Map<String, List<String>> base) {
        this.base = base;
    }

	@Override
	public int compare(String o1, String o2) {
		// TODO Auto-generated method stub
		int i1 = base.get(o1).size();
		int i2 = base.get(o2).size();
		return i1 >= i2 ? -1 : 1;
	}
}