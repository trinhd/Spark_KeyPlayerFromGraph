package islab.keyplayer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
				String[] coor = null; //{ segment.getStartVertex(), segment.getEndVertex() };
				BigDecimal bdTemp = null; //mapResult.get(coor);
				for (Entry<String[], BigDecimal> entry : mapResult.entrySet()) {
					coor = entry.getKey();
					if (coor[0].equals(segment.getStartVertex()) && coor[1].equals(segment.getEndVertex())){
						bdTemp = entry.getValue();
						break;
					}
				}
				if (bdTemp != null) {
					bdTemp = bdTemp.multiply(BigDecimal.ONE.subtract(segment.getIndirectInfluence()));
					mapResult.replace(coor, bdTemp);
				}
				else {
					bdTemp = BigDecimal.ONE.subtract(segment.getIndirectInfluence());
					mapResult.putIfAbsent(new String[]{segment.getStartVertex(), segment.getEndVertex()}, bdTemp);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
