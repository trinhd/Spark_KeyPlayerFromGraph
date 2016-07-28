package islab.keyplayer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatrixResultFromSegment extends Thread {
	private Map<String[], BigDecimal> mapResult = new HashMap<String[], BigDecimal>();
	private List<Segment> listSegment = new ArrayList<Segment>();

	public MatrixResultFromSegment(Map<String[], BigDecimal> mapResult, List<Segment> listSegment) {
		// TODO Auto-generated constructor stub
		this.mapResult = mapResult;
		this.listSegment = listSegment;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			for (Segment segment : listSegment) {
				String[] coor = { segment.getStartVertex(), segment.getEndVertex() };
				BigDecimal bdTemp = mapResult.get(coor);
				if (bdTemp != null) {
					bdTemp = bdTemp.multiply(BigDecimal.ONE.subtract(segment.getIndirectInfluence()));
					mapResult.replace(coor, bdTemp);
				}
				else {
					bdTemp = BigDecimal.ONE.subtract(segment.getIndirectInfluence());
					mapResult.putIfAbsent(coor, bdTemp);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
