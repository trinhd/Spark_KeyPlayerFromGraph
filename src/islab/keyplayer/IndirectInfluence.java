package islab.keyplayer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class IndirectInfluence implements Serializable {
	private JavaPairRDD<String, JavaRDD<String>> mOverThresholdInfluenceVertices;
		
	public IndirectInfluence() {
		this.mOverThresholdInfluenceVertices = null;
	}
	
	public void addNewVertex(String vName, JavaRDD<String> listVertex) {
		Tuple2<String, JavaRDD<String>> oneVertex = new Tuple2<String, JavaRDD<String>>(vName, listVertex);
		List<Tuple2<String, JavaRDD<String>>> list = new ArrayList<Tuple2<String, JavaRDD<String>>>();
		list.add(oneVertex);
		if (this.mOverThresholdInfluenceVertices != null) {
			this.mOverThresholdInfluenceVertices = this.mOverThresholdInfluenceVertices
					.union(KeyPlayer.sc.parallelizePairs(list));
		} else {
			this.mOverThresholdInfluenceVertices = KeyPlayer.sc.parallelizePairs(list);
		}
	}
	
	public long lCount(){
		return this.mOverThresholdInfluenceVertices.count();
	}
	
	public JavaRDD<String> getListVertexByName(String sName){
		JavaPairRDD<String, JavaRDD<String>> result = this.mOverThresholdInfluenceVertices.filter(arg0 -> arg0._1.equals(sName));
		
		return result.first()._2;
	}
		
	public void sortByVertexNumber(){
		if (!Data.flagSorted) {
			JavaPairRDD<JavaRDD<String>, String> temp = this.mOverThresholdInfluenceVertices.mapToPair(t -> t.swap());
			
			temp.sortByKey(new ValueComparator());
			
			this.mOverThresholdInfluenceVertices = temp.mapToPair(t -> t.swap());
			
			Data.flagSorted = true;
		}
	}
	
	public JavaRDD<String> getListVertexByIndex(int index) {
		return this.mOverThresholdInfluenceVertices.collect().get(index)._2;
	}
	
	public String getNameVertexByIndex(int index) {
		return this.mOverThresholdInfluenceVertices.collect().get(index)._1;
	}
}
