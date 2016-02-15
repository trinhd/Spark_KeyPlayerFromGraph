package islab.keyplayer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class IndirectInfluence implements Serializable {
	private Map<String, List<String>> mOverThresholdInfluenceVertices;
	//private String sName;
	//private List<String> arrOverThresholdInfluenceVertices;
	
	public IndirectInfluence() {
		this.mOverThresholdInfluenceVertices = new TreeMap<String, List<String>>();
	}
	
	public void addNewVertex(String vName, List<String> listVertex) {
		this.mOverThresholdInfluenceVertices.putIfAbsent(vName, listVertex);
	}
	
	public int iCount(){
		return this.mOverThresholdInfluenceVertices.size();
	}
	
	public List<String> getListVertexByName(String sName){
		return this.mOverThresholdInfluenceVertices.get(sName);
	}
	
	public int getVertexInfluenceByName(String sName){
		return getListVertexByName(sName).size();
	}
	
	public void addVertexToList(String srcName, String vName){
		List<String> list = getListVertexByName(srcName);
		list.add(vName);
	}
	
	public void sortByVertexNumber(){
		if (!Data.flagSorted) {
			Map<String, List<String>> sortedMap = new TreeMap<String, List<String>>(
					new ValueComparator(mOverThresholdInfluenceVertices));
			sortedMap.putAll(mOverThresholdInfluenceVertices);
			this.mOverThresholdInfluenceVertices = sortedMap;
			Data.flagSorted = true;
		}
	}
	
	public List<String> getListVertexByIndex(int index) {
		List<Entry<String, List<String>>> all = new ArrayList<Entry<String, List<String>>>(this.mOverThresholdInfluenceVertices.entrySet());
		return all.get(index).getValue();
	}
	
	public String getNameVertexByIndex(int index) {
		List<Entry<String, List<String>>> all = new ArrayList<Entry<String, List<String>>>(this.mOverThresholdInfluenceVertices.entrySet());
		return all.get(index).getKey();
	}
	
	@Override
	public String toString() {
		return Arrays.toString(this.mOverThresholdInfluenceVertices.entrySet().toArray());
	}
}
