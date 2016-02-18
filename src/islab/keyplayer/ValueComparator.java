package islab.keyplayer;

import java.util.Comparator;
import java.util.List;

public class ValueComparator implements Comparator<List<String>> {
	
	@Override
	public int compare(List<String> o1, List<String> o2) {
		// TODO Auto-generated method stub
		int i1 = o1.size();
		int i2 = o2.size();
		return i1 >= i2 ? -1 : 1;
	}
}