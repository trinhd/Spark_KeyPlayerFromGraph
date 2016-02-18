package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Utils implements Serializable{
	private Broadcast<JavaRDD<Vertex>> bcVertices;
	private Broadcast<JavaRDD<Edge>> bcEdges;
	private JavaPairRDD<String, List<String>> indirectInfluence;
	private ArrayList<String> result;
	private long lCount;//đếm số lượng tổ hợp phải duyệt qua

	public Utils(Broadcast<JavaRDD<Vertex>> bcVertices, Broadcast<JavaRDD<Edge>> bcEdges){
		this.indirectInfluence = null;
		this.result = null;
		this.lCount = 0;
		this.bcVertices = bcVertices;
		this.bcEdges = bcEdges;
	}
	
	public Vertex getVertexFromName(String sName) {
		JavaRDD<Vertex> vertices = bcVertices.value();
		//if (!vertices.isEmpty()) {
			JavaRDD<Vertex> v = vertices.filter(arg0 -> arg0.getName().equals(sName));
			return v.first();
		//}
		//return null;
	}

	public BigDecimal getVertexSpreadCoefficientFromName(String sName, String sStartName) {
		Vertex vertex = getVertexFromName(sName);

		if (vertex != null) {
			return vertex.getSpreadCoefficientFromVertexName(sStartName);
		}
		return new BigDecimal("-1");
	}

	public Edge getEdgeFromStartEndVertex(String sStart, String sEnd) {
		JavaRDD<Edge> edges = bcEdges.value();
		//if (!edges.isEmpty()) {
			JavaRDD<Edge> e = edges.filter(arg0 -> (arg0.getStartVertexName().equals(sStart) && arg0.getEndVertexName().equals(sEnd)));
			return e.first();
		//}
		//return null;
	}

	public BigDecimal getEdgeDirectInfluenceFromStartEndVertex(String sStart, String sEnd) {
		Edge edge = getEdgeFromStartEndVertex(sStart, sEnd);
		if (edge != null) {
			return edge.getDirectInfluence();
		}
		return new BigDecimal("-1");
	}

	public JavaRDD<Edge> getEdgesStartAtVertex(String sStartVertexName) {
		JavaRDD<Edge> listEdge = null;
		JavaRDD<Edge> edges = bcEdges.value();
		
		//if (!edges.isEmpty()) {
			listEdge = edges.filter(arg0 -> arg0.getStartVertexName().equals(sStartVertexName));
		//}
		return listEdge;
	}

	public JavaRDD<Edge> getEdgesEndAtVertex(String sEndVertexName) {
		JavaRDD<Edge> listEdge = null;
		JavaRDD<Edge> edges = bcEdges.value();
		//edges.cache();
		//System.out.println("--------------------------------->>>>>>EEEEEEEEEEEEEEEE" + edges.toString());
		//if (!edges.isEmpty()) {
			listEdge = edges.filter(arg0 -> arg0.getEndVertexName().equals(sEndVertexName));
		//}
		return listEdge;
	}

	public JavaRDD<String> getVerticesPointedByVertex(String sVertexName) {
		JavaRDD<String> result = null;
		JavaRDD<Edge> listEdges = getEdgesStartAtVertex(sVertexName);

		result = listEdges.map(e -> e.getEndVertexName());

		return result;
	}

	public JavaRDD<List<String>> getAllPathBetweenTwoVertex(String sStart, String sEnd) {
		List<List<String>> result = new ArrayList<List<String>>(); // ket qua
																	// tra ve la
																	// tat ca
																	// path
		//JavaRDD<Edge> edges = bcEdges.value();
		
		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		temp.add(sStart); // tham S
		explored.add(sStart); // danh dau S da tham
		JavaRDD<String> rddTemp = getVerticesPointedByVertex(sStart);
		List<String> listCandidate = new ArrayList<String>();
		cloneStringList(rddTemp.collect(), listCandidate);
		List<Integer> iCandidate = new ArrayList<Integer>();
		iCandidate.add(listCandidate.size());

		while (!listCandidate.isEmpty()) {
			int iCount = listCandidate.size();
			String sCandidate = listCandidate.remove(iCount - 1);
			int iLast = iCandidate.size() - 1;
			iCandidate.set(iLast, iCandidate.get(iLast) - 1);
			if (!explored.contains(sCandidate)) {
				temp.add(sCandidate);
				explored.add(sCandidate);
				if (sCandidate.equals(sEnd)) {
					result.add((List<String>)(((ArrayList<String>)(temp)).clone()));
					/*List<List<String>> temp2 = new ArrayList<List<String>>();
					temp2.add(temp);
					result = result.union(KeyPlayer.sc.parallelize(temp2));*/
					explored.remove(sCandidate);
					temp.remove(sCandidate);
					while (!iCandidate.isEmpty() && iCandidate.get(iCandidate.size() - 1) == 0) {
						temp.remove(temp.size() - 1);
						explored.remove(explored.size() - 1);
						iCandidate.remove(iCandidate.size() - 1);
					}
				} else {
					List<String> listNewCandidate = getVerticesPointedByVertex(sCandidate).collect();
					if (listNewCandidate != null && !listNewCandidate.isEmpty()) {
						listCandidate.addAll(listNewCandidate);
						iCandidate.add(listNewCandidate.size());
					} else {
						temp.remove(sCandidate);
						explored.remove(sCandidate);
						while (iCandidate.get(iCandidate.size() - 1) == 0) {
							temp.remove(temp.size() - 1);
							explored.remove(explored.size() - 1);
							iCandidate.remove(iCandidate.size() - 1);
						}
					}
				}
			} else {
				while (!iCandidate.isEmpty() && iCandidate.get(iCandidate.size() - 1) == 0) {
					temp.remove(temp.size() - 1);
					explored.remove(explored.size() - 1);
					iCandidate.remove(iCandidate.size() - 1);
				}
			}
		}
		
		//System.out.println(result);
		if ((result != null) && (!result.isEmpty())){
			JavaRDD<List<String>> rddResult = KeyPlayer.sc.parallelize(result);
			rddResult.cache();
			return rddResult;
		}
		return null;

		//return ((result != null) && (!result.isEmpty())) ? KeyPlayer.sc.parallelize(result) : null;
	}
	
	public class BigDecimalAccumulatorParam implements AccumulatorParam<BigDecimal> {

		@Override
		public BigDecimal addInPlace(BigDecimal arg0, BigDecimal arg1) {
			// TODO Auto-generated method stub
			BigDecimal res = arg0.add(arg1);
			return res;
		}

		@Override
		public BigDecimal zero(BigDecimal arg0) {
			// TODO Auto-generated method stub
			return BigDecimal.ZERO;
		}

		@Override
		public BigDecimal addAccumulator(BigDecimal arg0, BigDecimal arg1) {
			// TODO Auto-generated method stub
			BigDecimal res = arg0.add(arg1);
			return res;
		}
		
	}

	public BigDecimal IndirectInfluenceOfVertexOnOtherVertex(String sStartName, String sEndName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
		//JavaRDD<Vertex> vertices = bcGraph.getValue().getVertices();
		//JavaRDD<Edge> edges = bcGraph.getValue().getEdges();
		
		JavaRDD<List<String>> rddAllPath = getAllPathBetweenTwoVertex(sStartName, sEndName);
		
		if (rddAllPath != null) {

			List<List<String>> allpath = rddAllPath.collect();
			if (allpath != null) {
				/*
				 * CÁCH 1 - FAIL JavaRDD<BigDecimal> rddResult =
				 * allpath.map(path -> { BigDecimal bdResult = BigDecimal.ZERO;
				 * String sBefore = null; for (String v : path) { if (sBefore !=
				 * null) { bdResult =
				 * bdResult.add(getVertexSpreadCoefficientFromName(v, sBefore)
				 * .multiply(getEdgeDirectInfluenceFromStartEndVertex(sBefore,
				 * v))); if (bdResult.doubleValue() >= 1.0) { return
				 * BigDecimal.ONE; } } sBefore = v; }
				 * 
				 * return bdResult; }); //rddResult.foreach(arg0 ->
				 * System.out.println(arg0));
				 * System.out.println(rddResult.toDebugString());
				 * //KeyPlayer.sc.cancelAllJobs(); fIndirectInfluence =
				 * rddResult.reduce((arg0, arg1) -> { // TODO Auto-generated
				 * method stub BigDecimal res = arg0.add(arg1); return res; });
				 * 
				 * if (fIndirectInfluence.compareTo(BigDecimal.ONE) == 1){
				 * fIndirectInfluence = BigDecimal.ONE; }
				 */

				/*
				 * CÁCH 2 - FAIL final Accumulator<BigDecimal> accResult =
				 * KeyPlayer.sc.accumulator(BigDecimal.ZERO, new
				 * BigDecimalAccumulatorParam()); allpath.foreach(path -> {
				 * String sBefore = null; for (String v : path) { if (sBefore !=
				 * null) { accResult.add(getVertexSpreadCoefficientFromName(v,
				 * sBefore)
				 * .multiply(getEdgeDirectInfluenceFromStartEndVertex(sBefore,
				 * v))); } sBefore = v; } }); fIndirectInfluence =
				 * accResult.value(); if
				 * (fIndirectInfluence.compareTo(BigDecimal.ONE) == 1) {
				 * fIndirectInfluence = BigDecimal.ONE; }
				 */

				/*
				 * CÁCH 3 - FAIL JavaPairRDD<BigDecimal, BigDecimal> pairRes =
				 * allpath.flatMapToPair(path -> { String sBefore = null;
				 * List<Tuple2<BigDecimal, BigDecimal>> iteRes = new
				 * ArrayList<Tuple2<BigDecimal,BigDecimal>>(); for (String v :
				 * path) { if (sBefore != null) { iteRes.add(new
				 * Tuple2<BigDecimal,
				 * BigDecimal>(getVertexSpreadCoefficientFromName(v, sBefore),
				 * getEdgeDirectInfluenceFromStartEndVertex(sBefore, v))); }
				 * sBefore = v; } return iteRes; });
				 * 
				 * fIndirectInfluence = pairRes.map(arg0 ->
				 * arg0._1.multiply(arg0._2)).reduce((tri0, tri1) ->
				 * tri0.add(tri1)); if
				 * (fIndirectInfluence.compareTo(BigDecimal.ONE) == 1) {
				 * fIndirectInfluence = BigDecimal.ONE; }
				 */

				for (List<String> path : allpath) {
					String sBefore = null;
					for (String v : path) {
						if (sBefore != null) {
							fIndirectInfluence = fIndirectInfluence.add(getVertexSpreadCoefficientFromName(v, sBefore)
									.multiply(getEdgeDirectInfluenceFromStartEndVertex(sBefore, v)));
							if (fIndirectInfluence.doubleValue() >= 1.0) {
								return BigDecimal.ONE;
							}
						}
						sBefore = v;
					}
				}
			}
		}

		return fIndirectInfluence;
	}
	

	private BigDecimal IndirectInfluenceOfVertexOnAllVertex(String sVertexName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
		List<String> OverThresholdVertex = new ArrayList<String>();
		List<Vertex> vertices = bcVertices.value().collect();

		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			if (!vName.equals(sVertexName)) {
				BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(sVertexName, vName);
				fIndirectInfluence = fIndirectInfluence.add(bd);
				if (bd.compareTo(Data.theta.getValue()) != -1) {
					OverThresholdVertex.add(vName);
				}
			}
		}
		
		if (indirectInfluence != null){
			indirectInfluence = indirectInfluence.union(KeyPlayer.sc.parallelizePairs(Arrays.asList(new Tuple2<String, List<String>>(sVertexName, OverThresholdVertex))));
		}
		else{
			indirectInfluence = KeyPlayer.sc.parallelizePairs(Arrays.asList(new Tuple2<String, List<String>>(sVertexName, OverThresholdVertex)));
		}
		return fIndirectInfluence;
	}

	public JavaPairRDD<String, BigDecimal> getAllInfluenceOfVertices() {
		List<Tuple2<String, BigDecimal>> mUnsortedAll = new ArrayList<Tuple2<String, BigDecimal>>();
		List<Vertex> vertices = bcVertices.value().collect();

		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			mUnsortedAll.add(new Tuple2<String, BigDecimal>(vName, IndirectInfluenceOfVertexOnAllVertex(vName)));
		}
		
		/*mUnsortedAll = vertices.mapToPair(vertex -> {
			String vName = vertex.getName();
			return new Tuple2<String, BigDecimal>(vName, IndirectInfluenceOfVertexOnAllVertex(vName));
		});*/

		/*Map<String, BigDecimal> mSortedAll = new TreeMap<String, BigDecimal>(new ValueComparator2(mUnsortedAll));
		mSortedAll.putAll(mUnsortedAll);*/
		/*mUnsortedAll.entrySet().stream().sorted(Map.Entry.comparingByValue())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));*/
		
		JavaPairRDD<BigDecimal, String> mSortedAll = KeyPlayer.sc.parallelizePairs(mUnsortedAll).mapToPair(t -> t.swap());
		
		mSortedAll.sortByKey(new ValueComparator2());
		
		return mSortedAll.mapToPair(t -> t.swap());

		//return mUnsortedAll;
	}
	

	public JavaPairRDD<String, List<String>> getIndirectInfluence() {
		if (this.indirectInfluence.count() > 1 && !Data.flagSorted){
			JavaPairRDD<List<String>, String> pairTemp = this.indirectInfluence.mapToPair(arg0 -> arg0.swap());
			pairTemp.sortByKey(new ValueComparator());
			
			this.indirectInfluence = pairTemp.mapToPair(arg0 -> arg0.swap());
			Data.flagSorted = true;
		}
			
		return this.indirectInfluence;
	}
	
	public String getKeyPlayer() {
		return indirectInfluence.first()._1;
	}
	
	public List<String> getSmallestGroup() {
		long lOrgSize = bcVertices.value().count();
		
		getAllInfluenceOfVertices();
		if (this.indirectInfluence.count() > 1 && !Data.flagSorted){
			JavaPairRDD<List<String>, String> pairTemp = this.indirectInfluence.mapToPair(arg0 -> arg0.swap());
			pairTemp.sortByKey(new ValueComparator());
			
			this.indirectInfluence = pairTemp.mapToPair(arg0 -> arg0.swap());
			Data.flagSorted = true;
		}
		
		long lEdgesCount = bcEdges.value().count();

		for (int i = 1; i < lOrgSize; i++) {
			if (result == null) {
				getCombinations(i, lEdgesCount);
			} else {
				break;
			}
		}
		
		System.out.println("--------------------------------->>>>>>Số tổ hợp phải duyệt qua là: " + lCount);

		return result;
	}
	
	public void getCombinations(int k, long lMax) {
		long n = lMax;
		int a[] = new int[((int)n + 1)];
		for (int t = 1; t <= k; t++) {
			a[t] = t;
		}
		int i = 0;
		do {
			lCount++;
			//PrintCombine(a, k);
			int iTong = 0;
			for (int l = 1; l <= k; l++) {
				iTong += indirectInfluence.collect().get(a[l] - 1)._2.size();
			}
			if (iTong >= Data.iNeed.getValue()) {
				List<String> lMem = new ArrayList<String>();
				for (int l = 1; l <= k; l++) {
					for (String string : indirectInfluence.collect().get(a[l] - 1)._2) {
						if (!lMem.contains(string)) {
							lMem.add(string);
						}
					}
				}
				if (lMem.size() >= Data.iNeed.getValue()) {
					//System.out.println("Được");				
					//if (result == null) {
						result = new ArrayList<String>(k);
						for (int l = 1; l <= k; l++) {
							result.add(indirectInfluence.collect().get(a[l] - 1)._1);
						}
					//}
					break;
				} else {
					//System.out.println("Không được");
				}
			} else {
				//System.out.println("Không được và cắt");
				n = a[k] - 1;
			}
			i = k;
			while ((i > 0) && (a[i] >= n - k + i)) {
				--i;
			}
			if (i > 0) {
				a[i]++;
				for (int j = i + 1; j <= k; j++) {
					a[j] = a[j - 1] + 1;
				}
			}
		} while (i != 0 && result == null);
	}
	
	private void cloneStringList(List<String> scr, List<String> des) {
		for (String item : scr) {
			des.add(item);
		}
	}
	
	public String GraphToString(){
		//vertices.cache();
		//edges.cache();
		
		JavaRDD<Vertex> vertices = bcVertices.value();
		JavaRDD<Edge> edges = bcEdges.value();
		
		String sResult = new String("Vertices:");
		List<Vertex> listVertex = vertices.collect();
		for (Vertex vertex : listVertex) {
			sResult += "\nName:" + vertex.getName();
			sResult += "\nSpreadCoefficiency: {\n";
			Map<String, BigDecimal> msc = vertex.getSpreadCoefficient();
			if (msc != null && !msc.isEmpty()){
				sResult += Arrays.toString(msc.entrySet().toArray());
			}
			sResult += "\n}";
		}
		sResult += "\nEdges:\n";
		List<Edge> listEdge = edges.collect();	
		for (Edge edge : listEdge) {
			sResult += "Start: " + edge.getStartVertexName() + ", End: " + edge.getEndVertexName() + ", DirectInfluence: " + edge.getDirectInfluence().toString() + "\n";
		}
		return sResult;
	}
}
