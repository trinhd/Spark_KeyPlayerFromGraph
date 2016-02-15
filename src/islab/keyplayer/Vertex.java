package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import com.google.gson.annotations.SerializedName;

import scala.Tuple2;

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
		JavaPairRDD<String, BigDecimal> rddResult = mSpreadCoefficient.filter(new Function<Tuple2<String, BigDecimal>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, BigDecimal> arg0) throws Exception {
				return arg0._1.equals(sName);
			}
		});
		
		return rddResult.first()._2;
	}
}
