package islab.keyplayer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Graph implements Serializable {
	private List<Vertex> vertices;
	private List<Edge> edges;

	public Graph() {
		this.vertices = new ArrayList<Vertex>();
		this.edges = new ArrayList<Edge>();
	}
	
	public Graph(int iCountVertex, int iCountEdge) {
		this.vertices = new ArrayList<Vertex>(iCountVertex);
		this.edges = new ArrayList<Edge>(iCountEdge);
	}
	
	public void addVertex(Vertex vertex) {
		vertices.add(vertex);
	}

	public void addEdge(Edge edge) {
		edges.add(edge);
	}

	public int countVertex() {
		return vertices.size();
	}

	public int countEdge() {
		return edges.size();
	}

	public List<Vertex> getVertices(){
		return this.vertices;
	}
	
	public List<Edge> getEdges() {
		return this.edges;
	}
}
