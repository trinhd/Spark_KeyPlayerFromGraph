package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import akka.japi.Pair;
import scala.Tuple2;

public class Utils implements Serializable{
	//private Broadcast<JavaRDD<Vertex>> bcVertices;
	//private Broadcast<JavaRDD<Edge>> bcEdges;
	//private List<Vertex> vertices;
	//private List<Edge> edges;
	private JavaPairRDD<String, List<String>> indirectInfluence;
	private List<Tuple2<String, Integer>> indirectInfluenceCount;
	private ArrayList<String> result;
	private long lCount;//đếm số lượng tổ hợp phải duyệt qua
	private transient JavaSparkContext sc;

	public Utils(JavaSparkContext sc){
		this.indirectInfluence = null;
		this.indirectInfluenceCount = null;
		this.result = null;
		this.lCount = 0;
		//this.vertices = vertices;
		//this.edges = edges;
		this.sc = sc;
	}
	
	public Vertex getVertexFromName(List<Vertex> vertices, String sName) {
		//List<Vertex> vertices = bcVertices.value().collect();
		for (Vertex vertex : vertices) {
			if (vertex.getName().equals(sName)) {
				return vertex;
			}
		}
		return null;
	}

	public BigDecimal getVertexSpreadCoefficientFromName(List<Vertex> vertices, String sName, String sStartName) {
		Vertex vertex = getVertexFromName(vertices, sName);

		if (vertex != null) {
			return vertex.getSpreadCoefficientFromVertexName(sStartName);
		}
		return new BigDecimal("-1");
	}

	public Edge getEdgeFromStartEndVertex(List<Edge> edges, String sStart, String sEnd) {
		for (Edge edge : edges) {
			if (edge.getStartVertexName().equals(sStart) && edge.getEndVertexName().equals(sEnd)) {
				return edge;
			}
		}
		return null;
	}

	public BigDecimal getEdgeDirectInfluenceFromStartEndVertex(List<Edge> edges, String sStart, String sEnd) {
		Edge edge = getEdgeFromStartEndVertex(edges, sStart, sEnd);
		if (edge != null) {
			return edge.getDirectInfluence();
		}
		return new BigDecimal("-1");
	}

	public List<Edge> getEdgesStartAtVertex(List<Edge> edges, String sStartVertexName) {
		List<Edge> listEdge = new ArrayList<Edge>();

		for (Edge edge : edges) {
			if (edge.getStartVertexName().equals(sStartVertexName)) {
				listEdge.add(edge);
			}
		}
		
		return listEdge;
	}

	public List<Edge> getEdgesEndAtVertex(List<Edge> edges, String sEndVertexName) {
		List<Edge> listEdge = new ArrayList<Edge>();

		for (Edge edge : edges) {
			if (edge.getEndVertexName().equals(sEndVertexName)) {
				listEdge.add(edge);
			}
		}
		
		return listEdge;
	}

	public List<String> getVerticesPointedByVertex(List<Edge> edges, String sVertexName) {
		List<String> result = new ArrayList<String>();
		List<Edge> listEdges = getEdgesStartAtVertex(edges, sVertexName);

		for (Edge edge : listEdges) {
			result.add(edge.getEndVertexName());
		}

		return result;
	}

	public List<List<String>> getAllPathBetweenTwoVertex(List<Edge> edges, String sStart, String sEnd) {
		List<List<String>> result = new ArrayList<List<String>>(); // ket qua
																	// tra ve la
																	// tat ca
																	// path
		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		temp.add(sStart); // tham S
		explored.add(sStart); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(edges, sStart);
		List<Integer> iCandidate = new ArrayList<Integer>();
		if (listCandidate != null) {
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
						//List<String> onePath = new ArrayList<String>(temp.size());
						//cloneStringList(temp, onePath);
						//onePath = (List<String>)(((ArrayList<String>)temp).clone());
						result.add((List<String>)(((ArrayList<String>)temp).clone()));
						explored.remove(sCandidate);
						temp.remove(sCandidate);
						while (!iCandidate.isEmpty() && iCandidate.get(iCandidate.size() - 1) == 0) {
							temp.remove(temp.size() - 1);
							explored.remove(explored.size() - 1);
							iCandidate.remove(iCandidate.size() - 1);
						}
					} else {
						List<String> listNewCandidate = getVerticesPointedByVertex(edges, sCandidate);
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
								if (iCandidate.isEmpty()){
									break;
								}
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
		}

		return ((result != null) && (!result.isEmpty())) ? result : null;
	}
	
	public BigDecimal IndirectInfluenceOfVertexOnOtherVertex(List<Vertex> vertices, List<Edge> edges, String sStartName, String sEndName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
		
		/*final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
		final Broadcast<List<Edge>> bcEdges =  sc.broadcast(edges);
				
		List<List<String>> listPath = getAllPathBetweenTwoVertex(bcEdges.value(), sStartName, sEndName);
		if (listPath != null) {
			JavaRDD<List<String>> rddAllPath = sc.parallelize(listPath);
			rddAllPath.cache();

			if (rddAllPath != null) {
				fIndirectInfluence = rddAllPath.map(path -> {
					BigDecimal bdTemp = BigDecimal.ZERO;
					String sBefore = null;
					for (String v : path) {
						if (sBefore != null) {
							bdTemp = bdTemp.add(getVertexSpreadCoefficientFromName(bcVertices.value(), v, sBefore)
									.multiply(getEdgeDirectInfluenceFromStartEndVertex(bcEdges.value(), sBefore, v)));
							if (bdTemp.compareTo(BigDecimal.ONE) == 1) {
								return BigDecimal.ONE;
							}
						}
						sBefore = v;
					}
					return bdTemp;
				}).reduce((bd1, bd2) -> bd1.add(bd2));
			}
		}*/
		
		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		temp.add(sStartName); // tham S
		explored.add(sStartName); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(edges, sStartName);
		List<Integer> iCandidate = new ArrayList<Integer>();
		if (listCandidate != null) {
			iCandidate.add(listCandidate.size());

			while (!listCandidate.isEmpty()) {
				int iCount = listCandidate.size();
				String sCandidate = listCandidate.remove(iCount - 1);
				int iLast = iCandidate.size() - 1;
				iCandidate.set(iLast, iCandidate.get(iLast) - 1);
				if (!explored.contains(sCandidate)) {
					temp.add(sCandidate);
					explored.add(sCandidate);
					if (sCandidate.equals(sEndName)) {
						String sBefore = null;
						for (String v : temp) {
							if (sBefore != null) {
								fIndirectInfluence = fIndirectInfluence.add(getVertexSpreadCoefficientFromName(vertices, v, sBefore)
										.multiply(getEdgeDirectInfluenceFromStartEndVertex(edges, sBefore, v)));
								if (fIndirectInfluence.compareTo(BigDecimal.ONE) != -1) {
									return BigDecimal.ONE;
								}
							}
							sBefore = v;
						}
						explored.remove(sCandidate);
						temp.remove(sCandidate);
						while (!iCandidate.isEmpty() && iCandidate.get(iCandidate.size() - 1) == 0) {
							temp.remove(temp.size() - 1);
							explored.remove(explored.size() - 1);
							iCandidate.remove(iCandidate.size() - 1);
						}
					} else {
						List<String> listNewCandidate = getVerticesPointedByVertex(edges, sCandidate);
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
								if (iCandidate.isEmpty()){
									break;
								}
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
		}

		//return (fIndirectInfluence.compareTo(BigDecimal.ONE) == 1) ? BigDecimal.ONE : fIndirectInfluence;
		return fIndirectInfluence;
	}
	
	private BigDecimal IndirectInfluenceOfVertexOnAllVertex(List<Vertex> vertices, List<Edge> edges, String sVertexName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
		/*List<String> OverThresholdVertex = new ArrayList<String>();
				
		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			if (!vName.equals(sVertexName)) {
				BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(vertices, edges, sVertexName, vName);
				fIndirectInfluence = fIndirectInfluence.add(bd);
				if (bd.compareTo(Data.theta) != -1) {
					OverThresholdVertex.add(vName);
				}
			}
		}
		
		if (indirectInfluence != null) {
			if (indirectInfluence.lookup(sVertexName).isEmpty()) {
				indirectInfluence = indirectInfluence.union(sc.parallelizePairs(
						Arrays.asList(new Tuple2<String, List<String>>(sVertexName, OverThresholdVertex))));//accOverThresholdVertex.value()))));
			}
		} else {
			indirectInfluence = sc.parallelizePairs(
					Arrays.asList(new Tuple2<String, List<String>>(sVertexName, OverThresholdVertex)));//accOverThresholdVertex.value())));
		}
		return fIndirectInfluence;//accBD.value();*/
		
		final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
		final Broadcast<List<Edge>> bcEdges =  sc.broadcast(edges);
		final Broadcast<String> bcVertexName = sc.broadcast(sVertexName);
		
		JavaRDD<Vertex> rddVertices = sc.parallelize(vertices);
		//rddVertices.cache();
		
		JavaPairRDD<String, BigDecimal> rddIndrInfl = rddVertices.mapToPair(vertex -> {
			String vName = vertex.getName();
			if (!vName.equals(bcVertexName.value())) {
				BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(bcVertices.value(), bcEdges.value(), bcVertexName.value(), vName);
				return new Tuple2<String, BigDecimal>(vName, bd);
			}
			else {
				return new Tuple2<String, BigDecimal>(vName, BigDecimal.ZERO);
			}
		});
		rddIndrInfl.cache();
		
		fIndirectInfluence = rddIndrInfl.values().reduce((bd1, bd2) -> bd1.add(bd2));
		
		final Broadcast<BigDecimal> bcTheta = sc.broadcast(Data.theta);
		
		/*JavaPairRDD<String,List<String>> OverThresholdVertex = rddIndrInfl.filter(tuple -> {
			return (tuple._2.compareTo(bcTheta.value()) != -1);
		}).mapToPair(pairSB ->{
			return new Tuple2<String, List<String>>(bcVertexName.value(), new ArrayList<String>(Arrays.asList(pairSB._1)));
		}).reduceByKey((l1, l2) -> {
			l1.addAll(l2);
			return l1;
		});*/
		
		JavaPairRDD<String,List<String>> OverThresholdVertex = sc.parallelizePairs(Arrays.asList(new Tuple2<String,List<String>>(sVertexName, rddIndrInfl.filter(tuple -> {
			return (tuple._2.compareTo(bcTheta.value()) != -1);
		}).keys().collect())));
		
		if (indirectInfluence != null) {
			//if (indirectInfluence.lookup(sVertexName).isEmpty()) {
				indirectInfluence = indirectInfluence.union(OverThresholdVertex);
			//}
		} else {
			indirectInfluence = OverThresholdVertex;
		}
		
		return fIndirectInfluence;
	}

	public JavaPairRDD<String, BigDecimal> getAllInfluenceOfVertices(List<Vertex> vertices, List<Edge> edges) {
		List<Tuple2<BigDecimal, String>> mUnsortedAll = new ArrayList<Tuple2<BigDecimal, String>>(vertices.size());
		//List<Vertex> vertices = bcVertices.value().collect();
		
		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			mUnsortedAll.add(new Tuple2<BigDecimal, String>(IndirectInfluenceOfVertexOnAllVertex(vertices, edges, vName), vName));
		}
		
		return sc.parallelizePairs(mUnsortedAll).sortByKey(false).mapToPair(t -> t.swap());
	}
	
	public JavaPairRDD<String, List<String>> getIndirectInfluence(List<Vertex> vertices, List<Edge> edges) {
				
		if (this.indirectInfluence == null){
			getAllInfluenceOfVertices(vertices, edges);
		}
		
		if (this.indirectInfluence.count() > 1 && !Data.flagSorted) {
			indirectInfluenceCount = indirectInfluence.mapToPair(tuple -> {
				return new Tuple2<Integer, String>(tuple._2.size(), tuple._1);
			}).sortByKey(false).mapToPair(tuple -> tuple.swap()).collect();
			Data.flagSorted = true;
		}
			
		return this.indirectInfluence;
	}
	
	public String getTheMostOverThresholdVertexName(List<Vertex> vertices, List<Edge> edges) {
		if (indirectInfluenceCount == null){
			getAllInfluenceOfVertices(vertices, edges);
		}
		return indirectInfluenceCount.get(0)._1;
	}
	
	public List<String> getSmallestGroup(List<Vertex> vertices, List<Edge> edges) {
		int iOrgSize = vertices.size();
		
		getIndirectInfluence(vertices, edges);
		
		int iEdgesCount = edges.size();

		for (int i = 1; i < iOrgSize; i++) {
			if (result == null) {
				getCombinations(i, iEdgesCount);
			} else {
				break;
			}
		}
		
		System.out.println("Số tổ hợp phải duyệt qua là: " + lCount);

		return result;
	}
	
	public void getCombinations(int k, int iMax) {
		int n = iMax;
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
				iTong += indirectInfluenceCount.get(a[l] - 1)._2;
			}
			if (iTong >= Data.iNeed) {
				List<String> lMem = new ArrayList<String>();
				for (int l = 1; l <= k; l++) {
					String str = indirectInfluenceCount.get(a[l] - 1)._1;
					List<String> lTemp = indirectInfluence.lookup(str).get(0);
					for (String string : lTemp) {
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
							result.add(indirectInfluenceCount.get(a[l] - 1)._1);
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
	
	/*private void cloneStringList(List<String> scr, List<String> des) {
		for (String item : scr) {
			des.add(item);
		}
	}*/
	
	public String GraphToString(List<Vertex> vertices, List<Edge> edges){		
		String sResult = new String("Vertices:");
		for (Vertex vertex : vertices) {
			sResult += "\nName:" + vertex.getName();
			sResult += "\nSpreadCoefficiency: {\n";
			Map<String, BigDecimal> msc = vertex.getSpreadCoefficient();
			if (msc != null && !msc.isEmpty()){
				sResult += Arrays.toString(msc.entrySet().toArray());
			}
			sResult += "\n}";
		}
		sResult += "\nEdges:\n";	
		for (Edge edge : edges) {
			sResult += "Start: " + edge.getStartVertexName() + ", End: " + edge.getEndVertexName() + ", DirectInfluence: " + edge.getDirectInfluence().toString() + "\n";
		}
		return sResult;
	}
}
