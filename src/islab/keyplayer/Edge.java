package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;

import com.google.gson.annotations.SerializedName;

public class Edge implements Serializable{
	@SerializedName("Start")
	private final String sStart;
	@SerializedName("End")
	private final String sEnd;
	@SerializedName("DirectInfluence")
	private final BigDecimal fDirectInfluence; // Suc anh huong truc tiep

	public Edge() {
		// TODO Auto-generated constructor stub
		this.sStart = "";
		this.sEnd = "";
		this.fDirectInfluence = BigDecimal.ZERO;
	}

	public Edge(String sStart, String sEnd, BigDecimal fDirectInfluence) {
		// TODO Auto-generated constructor stub
		this.sStart = sStart;
		this.sEnd = sEnd;
		this.fDirectInfluence = fDirectInfluence;
	}

	public BigDecimal getDirectInfluence() {
		return this.fDirectInfluence;
	}

	public String getStartVertexName() {
		return this.sStart;
	}

	public String getEndVertexName() {
		return this.sEnd;
	}
}
