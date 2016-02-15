package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Graph implements Serializable {
	private List<Vertex> vertices;
	private List<Edge> edges;
	private IndirectInfluence indirectInfluence;
	private ArrayList<String> result;
	private long lCount;//đếm số lượng tổ hợp phải duyệt qua

	public Graph() {
		this.vertices = new ArrayList<Vertex>();
		this.edges = new ArrayList<Edge>();
		this.indirectInfluence = new IndirectInfluence();
		new ArrayList<Integer>();
		this.result = null;
		this.lCount = 0;
	}

	public boolean addVertex(String sName, Map<String, BigDecimal> mSpreadCoefficient) {
		return this.getVertexFromName(sName) != null ? false : vertices.add(new Vertex(sName, mSpreadCoefficient));
	}

	public boolean addVertex(Vertex vertex) {
		return this.containVertex(vertex) ? false : vertices.add(vertex);
	}

	public boolean addEdge(Edge edge) {
		return this.containEdge(edge) ? false : edges.add(edge);
	}

	public boolean addEdge(String sStart, String sEnd, BigDecimal fDirectInfluence) {
		return this.getEdgeFromStartEndVertex(sStart, sEnd) != null ? false
				: edges.add(new Edge(sStart, sEnd, fDirectInfluence));
	}

	public int countVertex() {
		return vertices.size();
	}

	public int countEdge() {
		return edges.size();
	}

	public boolean containVertex(Vertex vertex) {
		return vertices.contains(vertex);
	}

	public Vertex getVertexFromName(String sName) {
		if (!vertices.isEmpty()) {
			for (Vertex vertex : vertices) {
				if (vertex.getName().equals(sName)) {
					return vertex;
				}
			}
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

	public boolean containEdge(Edge edge) {
		return edges.contains(edge);
	}

	public Edge getEdgeFromStartEndVertex(String sStart, String sEnd) {
		if (!edges.isEmpty()) {
			for (Edge edge : edges) {
				if (edge.getStartVertexName().equals(sStart) && edge.getEndVertexName().equals(sEnd)) {
					return edge;
				}
			}
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

	public List<Edge> getEdgesStartAtVertex(String sStartVertexName) {
		List<Edge> listEdge = new ArrayList<Edge>();

		if (!edges.isEmpty()) {
			for (Edge edge : edges) {
				if (edge.getStartVertexName().equals(sStartVertexName)) {
					listEdge.add(edge);
				}
			}
		}
		return listEdge;
	}

	public List<Edge> getEdgesEndAtVertex(String sEndVertexName) {
		List<Edge> listEdge = new ArrayList<Edge>();

		if (!edges.isEmpty()) {
			for (Edge edge : edges) {
				if (edge.getEndVertexName().equals(sEndVertexName)) {
					listEdge.add(edge);
				}
			}
		}
		return listEdge;
	}

	public List<Vertex> getVerticesPointedByVertex(Vertex vertex) {
		List<Vertex> result = new ArrayList<Vertex>();
		List<Edge> listEdges = getEdgesStartAtVertex(vertex.getName());

		for (Edge edge : listEdges) {
			result.add(getVertexFromName(edge.getEndVertexName()));
		}

		return result;
	}

	public List<String> getVerticesPointedByVertex(String sVertexName) {
		List<String> result = new ArrayList<String>();
		List<Edge> listEdges = getEdgesStartAtVertex(sVertexName);

		for (Edge edge : listEdges) {
			result.add(edge.getEndVertexName());
		}

		return result;
	}

	public List<List<String>> getAllPathBetweenTwoVertex(String sStart, String sEnd) {
		List<List<String>> result = new ArrayList<List<String>>(); // ket qua
																	// tra ve la
																	// tat ca
																	// path
		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		temp.add(sStart); // tham S
		explored.add(sStart); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(sStart);
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
					explored.remove(sCandidate);
					temp.remove(sCandidate);
					while (!iCandidate.isEmpty() && iCandidate.get(iCandidate.size() - 1) == 0) {
						temp.remove(temp.size() - 1);
						explored.remove(explored.size() - 1);
						iCandidate.remove(iCandidate.size() - 1);
					}
				} else {
					List<String> listNewCandidate = getVerticesPointedByVertex(sCandidate);
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

		List<List<String>> allpath = getAllPathBetweenTwoVertex(sStartName, sEndName);
		if (allpath != null) {
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

		return fIndirectInfluence;
	}

	private BigDecimal IndirectInfluenceOfVertexOnAllVertex(String sVertexName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
		List<String> OverThresholdVertex = new ArrayList<String>();

		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			if (!vName.equals(sVertexName)) {
				BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(sVertexName, vName);
				fIndirectInfluence = fIndirectInfluence.add(bd);
				if (bd.compareTo(Data.theta) != -1) {
					OverThresholdVertex.add(vName);
				}
			}
		}
		indirectInfluence.addNewVertex(sVertexName, OverThresholdVertex);
		return fIndirectInfluence;
	}

	public Map<String, BigDecimal> getAllInfluenceOfVertices() {
		Map<String, BigDecimal> mUnsortedAll = new HashMap<String, BigDecimal>();

		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			mUnsortedAll.put(vName, IndirectInfluenceOfVertexOnAllVertex(vName));
		}

		Map<String, BigDecimal> mSortedAll = new TreeMap<String, BigDecimal>(new ValueComparator2(mUnsortedAll));
		mSortedAll.putAll(mUnsortedAll);
		/*mUnsortedAll.entrySet().stream().sorted(Map.Entry.comparingByValue())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));*/

		return mSortedAll;
	}

	public IndirectInfluence getIndirectInfluence() {
		if (indirectInfluence.iCount() > 1)
			this.indirectInfluence.sortByVertexNumber();
		return this.indirectInfluence;
	}

	public String getKeyPlayer() {
		return indirectInfluence.getNameVertexByIndex(0);
	}

	public List<String> getSmallestGroup() {
		int iOrgSize = countVertex();
		
		getAllInfluenceOfVertices();
		if (indirectInfluence.iCount() > 1) {
			this.indirectInfluence.sortByVertexNumber();
		}

		for (int i = 1; i < iOrgSize; i++) {
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
		int n = countVertex();
		int a[] = new int[n + 1];
		for (int t = 1; t <= k; t++) {
			a[t] = t;
		}
		int i = 0;
		do {
			lCount++;
			//PrintCombine(a, k);
			int iTong = 0;
			for (int l = 1; l <= k; l++) {
				iTong += indirectInfluence.getListVertexByIndex(a[l] - 1).size();
			}
			if (iTong >= Data.iNeed) {
				List<String> lMem = new ArrayList<String>();
				for (int l = 1; l <= k; l++) {
					for (String string : indirectInfluence.getListVertexByIndex(a[l] - 1)) {
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
	
	@Override
	public String toString(){
		String sResult = new String("Vertices:\n");
		for (Vertex vertex : vertices) {
			sResult += "Name:" + vertex.getName() + "\n";
			sResult += "SpreadCoefficiency: {\n";
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
