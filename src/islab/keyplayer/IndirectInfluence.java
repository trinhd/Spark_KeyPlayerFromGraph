package islab.keyplayer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.TUGIBasedProcessor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class IndirectInfluence implements Serializable {
	//private Map<String, List<String>> mOverThresholdInfluenceVertices;
	private JavaPairRDD<String, JavaRDD<String>> mOverThresholdInfluenceVertices;
	//private String sName;
	//private List<String> arrOverThresholdInfluenceVertices;
	
	public IndirectInfluence() {
		this.mOverThresholdInfluenceVertices = null;
	}
	
	public void addNewVertex(String vName, JavaRDD<String> listVertex) {
		Tuple2<String, JavaRDD<String>> oneVertex = new Tuple2<String, JavaRDD<String>>(vName, listVertex);
		List<Tuple2<String, JavaRDD<String>>> list = new ArrayList<Tuple2<String, JavaRDD<String>>>();
		list.add(oneVertex);
		this.mOverThresholdInfluenceVertices = this.mOverThresholdInfluenceVertices.union(KeyPlayer.sc.parallelizePairs(list));
	}
	
	public long lCount(){
		return this.mOverThresholdInfluenceVertices.count();
	}
	
	public JavaRDD<String> getListVertexByName(String sName){
		JavaPairRDD<String, JavaRDD<String>> result = this.mOverThresholdInfluenceVertices.filter(new Function<Tuple2<String,JavaRDD<String>>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, JavaRDD<String>> arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0._1.equals(sName);
			}
		});
		
		return result.first()._2;
	}
	
	/*public int getVertexInfluenceByName(String sName){
		return getListVertexByName(sName).size();
	}*/
	
	/*public void addVertexToList(String srcName, String vName){
		List<String> list = getListVertexByName(srcName);
		list.add(vName);
	}*/
	
	public void sortByVertexNumber(){
		if (!Data.flagSorted) {
			JavaPairRDD<JavaRDD<String>, String> temp = this.mOverThresholdInfluenceVertices.mapToPair(new PairFunction<Tuple2<String,JavaRDD<String>>, JavaRDD<String>, String>() {
				@Override
				public Tuple2<JavaRDD<String>, String> call(Tuple2<String,JavaRDD<String>> t){
					return t.swap();
				}
			});
			
			temp.sortByKey(new ValueComparator());
			
			this.mOverThresholdInfluenceVertices = temp.mapToPair(new PairFunction<Tuple2<JavaRDD<String>,String>, String, JavaRDD<String>>() {
				@Override
				public Tuple2<String, JavaRDD<String>> call(Tuple2<JavaRDD<String>,String> t){
					return t.swap();
				}
			});
			
			Data.flagSorted = true;
		}
	}
	
	public JavaRDD<String> getListVertexByIndex(int index) {
		return this.mOverThresholdInfluenceVertices.collect().get(index)._2;
	}
	
	public String getNameVertexByIndex(int index) {
		return this.mOverThresholdInfluenceVertices.collect().get(index)._1;
	}
	
	/*@Override
	public String toString() {
		return Arrays.toString(this.mOverThresholdInfluenceVertices.entrySet().toArray());
	}*/
}
