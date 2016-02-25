package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class Vertex implements Serializable{
	@SerializedName("Name")
	private final String sName;
	@SerializedName("SpreadCoefficient")
	private Map<String, BigDecimal> mSpreadCoefficient; // He so lan truyen

	public Vertex() {
		// TODO Auto-generated constructor stub
		this.sName = "";
		this.mSpreadCoefficient = null;
	}

	public Vertex(String sName) {
		this.sName = sName;
		this.mSpreadCoefficient = null;
	}

	public Vertex(String sName, Map<String, BigDecimal> mSpreadCoefficient) {
		this.sName = sName;
		this.mSpreadCoefficient = mSpreadCoefficient;
	}

	public Vertex(String sName, String sStartVertexName, BigDecimal fSpreadCoefficientFromStartVertex) {
		this.sName = sName;
		this.mSpreadCoefficient = new HashMap<String, BigDecimal>();
		this.mSpreadCoefficient.put(sStartVertexName, fSpreadCoefficientFromStartVertex);
	}

	public boolean insertORreplaceSpreadCoefficient(String sStartVertexName,
			BigDecimal fSpreadCoefficientFromStartVertex) {
		BigDecimal fResult = this.mSpreadCoefficient.put(sStartVertexName, fSpreadCoefficientFromStartVertex);
		return fResult == null ? true : false;
	}

	public String getName() {
		return this.sName;
	}

	public Map<String, BigDecimal> getSpreadCoefficient() {
		return this.mSpreadCoefficient;
	}

	public BigDecimal getSpreadCoefficientFromVertexName(String sName) {
		return this.mSpreadCoefficient.get(sName);
	}
}
