package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import groovy.lang.Tuple;
import scala.Tuple2;

public class Graph implements Serializable {
	private JavaRDD<Vertex> vertices;
	private JavaRDD<Edge> edges;
	private IndirectInfluence indirectInfluence;
	private ArrayList<String> result;
	private long lCount;//đếm số lượng tổ hợp phải duyệt qua

	public Graph() {
		this.vertices = null;
		this.edges = null;
		this.indirectInfluence = new IndirectInfluence();
		this.result = null;
		this.lCount = 0;
	}

	/*public boolean addVertex(String sName, Map<String, BigDecimal> mSpreadCoefficient) {
		return this.getVertexFromName(sName) != null ? false : vertices.add(new Vertex(sName, mSpreadCoefficient));
	}*/

	public void addVertex(Vertex vertex) {
		List<Vertex> lTemp = new ArrayList<Vertex>();
		lTemp.add(vertex);
		this.vertices = this.vertices.union(KeyPlayer.sc.parallelize(lTemp));
	}

	public void addEdge(Edge edge) {
		List<Edge> lTemp = new ArrayList<Edge>();
		lTemp.add(edge);
		this.edges = this.edges.union(KeyPlayer.sc.parallelize(lTemp));
	}

	/*public boolean addEdge(String sStart, String sEnd, BigDecimal fDirectInfluence) {
		return this.getEdgeFromStartEndVertex(sStart, sEnd) != null ? false
				: edges.add(new Edge(sStart, sEnd, fDirectInfluence));
	}*/

	public long countVertex() {
		return vertices.count();
	}

	public long countEdge() {
		return edges.count();
	}

	/*public boolean containVertex(Vertex vertex) {
		return vertices.contains(vertex);
	}*/

	public Vertex getVertexFromName(String sName) {
		if (!vertices.isEmpty()) {
			JavaRDD<Vertex> v = vertices.filter(new Function<Vertex, Boolean>() {
				
				@Override
				public Boolean call(Vertex arg0) throws Exception {
					// TODO Auto-generated method stub
					return arg0.getName().equals(sName);
				}
			});
			return v.first();
		}
		return null;
	}

	public BigDecimal getVertexSpreadCoefficientFromName(String sName, String sStartName) {
		Vertex vertex = getVertexFromName(sName);

		if (vertex != null) {
			return vertex.getSpreadCoefficientFromVertexName(sStartName);
		}
		return new BigDecimal("-1");
	}

	/*public boolean containEdge(Edge edge) {
		return edges.contains(edge);
	}*/

	public Edge getEdgeFromStartEndVertex(String sStart, String sEnd) {
		if (!edges.isEmpty()) {
			JavaRDD<Edge> e = edges.filter(new Function<Edge, Boolean>() {
				
				@Override
				public Boolean call(Edge arg0) throws Exception {
					// TODO Auto-generated method stub
					return (arg0.getStartVertexName().equals(sStart) && arg0.getEndVertexName().equals(sEnd));
				}
			});
			return e.first();
		}
		return null;
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

		if (!edges.isEmpty()) {
			listEdge = edges.filter(new Function<Edge, Boolean>() {
				
				@Override
				public Boolean call(Edge arg0) throws Exception {
					// TODO Auto-generated method stub
					return arg0.getStartVertexName().equals(sStartVertexName);
				}
			});
		}
		return listEdge;
	}

	public JavaRDD<Edge> getEdgesEndAtVertex(String sEndVertexName) {
		JavaRDD<Edge> listEdge = null;

		if (!edges.isEmpty()) {
			listEdge = edges.filter(new Function<Edge, Boolean>() {
				
				@Override
				public Boolean call(Edge arg0) throws Exception {
					// TODO Auto-generated method stub
					return arg0.getEndVertexName().equals(sEndVertexName);
				}
			});
		}
		return listEdge;
	}

	/*public List<Vertex> getVerticesPointedByVertex(Vertex vertex) {
		List<Vertex> result = new ArrayList<Vertex>();
		List<Edge> listEdges = getEdgesStartAtVertex(vertex.getName());

		for (Edge edge : listEdges) {
			result.add(getVertexFromName(edge.getEndVertexName()));
		}

		return result;
	}*/

	public JavaRDD<String> getVerticesPointedByVertex(String sVertexName) {
		JavaRDD<String> result = null;
		JavaRDD<Edge> listEdges = getEdgesStartAtVertex(sVertexName);

		result = listEdges.map(new Function<Edge, String>() {
			@Override
			public String call(Edge e){
				return e.getEndVertexName();
			}
		});

		return result;
	}

	public JavaRDD<List<String>> getAllPathBetweenTwoVertex(String sStart, String sEnd) {
		JavaRDD<List<String>> result = null; // ket qua
																	// tra ve la
																	// tat ca
																	// path
		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		temp.add(sStart); // tham S
		explored.add(sStart); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(sStart).collect();
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
					//result.add((List<String>)(((ArrayList<String>)(temp)).clone()));
					List<List<String>> temp2 = new ArrayList<List<String>>();
					temp2.add(temp);
					result = result.union(KeyPlayer.sc.parallelize(temp2));
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

		return ((result != null) && (!result.isEmpty())) ? result : null;
	}

	public BigDecimal IndirectInfluenceOfVertexOnOtherVertex(String sStartName, String sEndName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;

		JavaRDD<List<String>> allpath = getAllPathBetweenTwoVertex(sStartName, sEndName);
		if (allpath != null) {
			JavaRDD<BigDecimal> rddResult = allpath.flatMap(new FlatMapFunction<List<String>, BigDecimal>() {
				@Override
				public Iterable<BigDecimal> call(List<String> path){
					BigDecimal bdResult = BigDecimal.ZERO;
					String sBefore = null;
					for (String v : path) {
						if (sBefore != null) {
							bdResult = bdResult.add(getVertexSpreadCoefficientFromName(v, sBefore)
									.multiply(getEdgeDirectInfluenceFromStartEndVertex(sBefore, v)));
							if (bdResult.doubleValue() >= 1.0) {
								return Arrays.asList(BigDecimal.ONE);
							}
						}
						sBefore = v;
					}
					
					return Arrays.asList(bdResult);
				}
			});
			
			fIndirectInfluence = rddResult.reduce(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
				
				@Override
				public BigDecimal call(BigDecimal arg0, BigDecimal arg1) throws Exception {
					// TODO Auto-generated method stub
					return arg0.add(arg1);
				}
			});
			
			if (fIndirectInfluence.compareTo(BigDecimal.ONE) == 1){
				fIndirectInfluence = BigDecimal.ONE;
			}
		}

		return fIndirectInfluence;
	}

	private BigDecimal IndirectInfluenceOfVertexOnAllVertex(String sVertexName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
		
		JavaPairRDD<String, BigDecimal> rddIndirectInfluence = vertices.mapToPair(new PairFunction<Vertex, String, BigDecimal>() {
			@Override
			public Tuple2<String, BigDecimal> call(Vertex vertex){
				String vName = vertex.getName();
				if (!vName.equals(sVertexName)) {
					BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(sVertexName, vName);
					return new Tuple2<String, BigDecimal>(vName, bd);
				}
				else {
					return new Tuple2<String, BigDecimal>(sVertexName, BigDecimal.ZERO);
				}
			}
		});
		
		rddIndirectInfluence.cache();
		
		JavaRDD<String> rddOverThresholdVertex = rddIndirectInfluence.filter(new Function<Tuple2<String,BigDecimal>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, BigDecimal> arg0) throws Exception {
				// TODO Auto-generated method stub
				return (arg0._2.compareTo(Data.theta) != -1);
			}
		}).keys();
		
		indirectInfluence.addNewVertex(sVertexName, rddOverThresholdVertex);
		
		fIndirectInfluence = rddIndirectInfluence.values().reduce(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
			
			@Override
			public BigDecimal call(BigDecimal arg0, BigDecimal arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0.add(arg1);
			}
		});
		
		return fIndirectInfluence;
	}

	public JavaPairRDD<String, BigDecimal> getAllInfluenceOfVertices() {
		JavaPairRDD<String, BigDecimal> mUnsortedAll = null;

		/*for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			mUnsortedAll.put(vName, IndirectInfluenceOfVertexOnAllVertex(vName));
		}*/
		
		mUnsortedAll = vertices.mapToPair(new PairFunction<Vertex, String, BigDecimal>() {
			@Override
			public Tuple2<String, BigDecimal> call(Vertex vertex){
				String vName = vertex.getName();
				return new Tuple2<String, BigDecimal>(vName, IndirectInfluenceOfVertexOnAllVertex(vName));
			}
		});

		/*Map<String, BigDecimal> mSortedAll = new TreeMap<String, BigDecimal>(new ValueComparator2(mUnsortedAll));
		mSortedAll.putAll(mUnsortedAll);*/
		/*mUnsortedAll.entrySet().stream().sorted(Map.Entry.comparingByValue())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));*/
		
		JavaPairRDD<BigDecimal, String> mSortedAll = mUnsortedAll.mapToPair(new PairFunction<Tuple2<String,BigDecimal>, BigDecimal, String>() {
			@Override
			public Tuple2<BigDecimal, String> call(Tuple2<String,BigDecimal> t){
				return t.swap();
			}
		});
		
		mSortedAll.sortByKey(new ValueComparator2());
		
		mUnsortedAll = mSortedAll.mapToPair(new PairFunction<Tuple2<BigDecimal,String>, String, BigDecimal>() {
			@Override
			public Tuple2<String, BigDecimal> call(Tuple2<BigDecimal,String> t){
				return t.swap();
			}
		});

		return mUnsortedAll;
	}

	public IndirectInfluence getIndirectInfluence() {
		if (indirectInfluence.lCount() > 1)
			this.indirectInfluence.sortByVertexNumber();
		return this.indirectInfluence;
	}
	

	public String getKeyPlayer() {
		return indirectInfluence.getNameVertexByIndex(0);
	}
	

	public List<String> getSmallestGroup() {
		long lOrgSize = countVertex();
		
		getAllInfluenceOfVertices();
		if (indirectInfluence.lCount() > 1) {
			this.indirectInfluence.sortByVertexNumber();
		}

		for (int i = 1; i < lOrgSize; i++) {
			if (result == null) {
				getCombinations(i);
			} else {
				break;
			}
		}
		
		System.out.println("--------------------------------->>>>>>Số tổ hợp phải duyệt qua là: " + lCount);

		return result;
	}
	

	public void getCombinations(int k) {
		long n = countVertex();
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
				iTong += indirectInfluence.getListVertexByIndex(a[l] - 1).count();
			}
			if (iTong >= Data.iNeed) {
				List<String> lMem = new ArrayList<String>();
				for (int l = 1; l <= k; l++) {
					for (String string : indirectInfluence.getListVertexByIndex(a[l] - 1).collect()) {
						if (!lMem.contains(string)) {
							lMem.add(string);
						}
					}
				}
				if (lMem.size() >= Data.iNeed) {
					//System.out.println("Được");				
					//if (result == null) {
						result = new ArrayList<String>(k);
						for (int l = 1; l <= k; l++) {
							result.add(indirectInfluence.getNameVertexByIndex(a[l] - 1));
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
	
	
	/*@Override
	public String toString(){
		vertices.cache();
		edges.cache();
		String sResult = new String("Vertices:\n");
		for (Vertex vertex : vertices.collect()) {
			sResult += "Name:" + vertex.getName() + "\n";
			sResult += "SpreadCoefficiency: {\n";
			Map<String, BigDecimal> msc = vertex.getSpreadCoefficient().collectAsMap();
			if (msc != null && !msc.isEmpty()){
				sResult += Arrays.toString(msc.entrySet().toArray());
			}
			sResult += "\n}";
		}
		sResult += "\nEdges:\n";
		for (Edge edge : edges.collect()) {
			sResult += "Start: " + edge.getStartVertexName() + ", End: " + edge.getEndVertexName() + ", DirectInfluence: " + edge.getDirectInfluence().toString() + "\n";
		}
		return sResult;
	}*/
}
