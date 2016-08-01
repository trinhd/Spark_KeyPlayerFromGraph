package islab.keyplayer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MatrixResultFromSegment extends Thread {
	private Map<String, Map<String, BigDecimal>> mapResult = new HashMap<String, Map<String, BigDecimal>>();
	private List<Segment> listSegment = new ArrayList<Segment>();

	public MatrixResultFromSegment(Map<String, Map<String, BigDecimal>> mapResult, List<Segment> listSegment) {
		// TODO Auto-generated constructor stub
		this.mapResult = mapResult;
		this.listSegment = listSegment;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			for (Segment segment : listSegment) {
				/*String[] coor = null; //{ segment.getStartVertex(), segment.getEndVertex() };
				BigDecimal bdTemp = null; //mapResult.get(coor);
				for (Entry<String[], BigDecimal> entry : mapResult.entrySet()) {
					coor = entry.getKey();
					if (coor[0].equals(segment.getStartVertex()) && coor[1].equals(segment.getEndVertex())){
						bdTemp = entry.getValue();
						break;
					}
				}*/
				BigDecimal bdTemp = null;
				boolean fStartVertexExist = false;
				Map<String, BigDecimal> mTemp = mapResult.get(segment.getStartVertex());
				if (mTemp != null){
					fStartVertexExist = true;
					bdTemp = mTemp.get(segment.getEndVertex());
				}
				
				if (bdTemp != null) {
					bdTemp = bdTemp.multiply(BigDecimal.ONE.subtract(segment.getIndirectInfluence()));
					mTemp.replace(segment.getEndVertex(), bdTemp);
					mapResult.replace(segment.getStartVertex(), mTemp);
				}
				else {
					bdTemp = BigDecimal.ONE.subtract(segment.getIndirectInfluence());
					if (!fStartVertexExist){
						mTemp = new HashMap<String, BigDecimal>();
						mTemp.put(segment.getEndVertex(), bdTemp);
						mapResult.put(segment.getStartVertex(), mTemp);
					}
					else {
						mTemp.put(segment.getEndVertex(), bdTemp);
						mapResult.replace(segment.getStartVertex(), mTemp);
					}
					//mapResult.putIfAbsent(new String[]{segment.getStartVertex(), segment.getEndVertex()}, bdTemp);
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
