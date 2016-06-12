package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;

public class Segment implements Serializable{
	private final String sStartVertex;
	private final String sEndVertex;
	private BigDecimal bdIndirectInfluence;
	private ArrayList<String> arrHistory;
	
	public Segment(String sStartVertex, String sEndVertex){
		this.sStartVertex = sStartVertex;
		this.sEndVertex = sEndVertex;
		this.bdIndirectInfluence = BigDecimal.ZERO;
		this.arrHistory = new ArrayList<String>();
	}
	
	public Segment(String sStartVertex, String sEndVertex, BigDecimal bdIndirectInfluence){
		this.sStartVertex = sStartVertex;
		this.sEndVertex = sEndVertex;
		this.bdIndirectInfluence = bdIndirectInfluence;
		this.arrHistory = new ArrayList<String>();
	}
	
	public Segment(String sStartVertex, String sEndVertex, BigDecimal bdIndirectInfluence, ArrayList<String> arrHistory){
		this.sStartVertex = sStartVertex;
		this.sEndVertex = sEndVertex;
		this.bdIndirectInfluence = bdIndirectInfluence;
		this.arrHistory = arrHistory;
	}
	
	public String getStartVertex(){
		return this.sStartVertex;
	}
	
	public String getEndVertex() {
		return this.sEndVertex;
	}
	
	public BigDecimal getIndirectInfluence() {
		return this.bdIndirectInfluence;
	}
	
	public ArrayList<String> getHistory(){
		return this.arrHistory;
	}
	
	public void addToHistory(String sVertex){
		this.arrHistory.add(sVertex);
	}
}