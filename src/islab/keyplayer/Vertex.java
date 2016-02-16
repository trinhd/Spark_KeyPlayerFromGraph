package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.JavaPairRDD;

import com.google.gson.annotations.SerializedName;

public class Vertex implements Serializable{
	@SerializedName("Name")
	private final String sName;
	@SerializedName("SpreadCoefficient")
	//private Map<String, BigDecimal> mSpreadCoefficient; // He so lan truyen
	private JavaPairRDD<String, BigDecimal> mSpreadCoefficient;
	
	public Vertex() {
		// TODO Auto-generated constructor stub
		this.sName = "";
		this.mSpreadCoefficient = null;
	}

	public Vertex(String sName) {
		this.sName = sName;
		this.mSpreadCoefficient = null;
	}

	public Vertex(String sName, JavaPairRDD<String, BigDecimal> mSpreadCoefficient) {
		this.sName = sName;
		this.mSpreadCoefficient = mSpreadCoefficient;
	}

	public String getName() {
		return this.sName;
	}

	public JavaPairRDD<String, BigDecimal> getSpreadCoefficient() {
		return this.mSpreadCoefficient;
	}

	public BigDecimal getSpreadCoefficientFromVertexName(String sName) {
		JavaPairRDD<String, BigDecimal> rddResult = mSpreadCoefficient.filter(arg0 -> arg0._1.equals(sName));
		
		return rddResult.first()._2;
	}
}
