package islab.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Graph implements Serializable {
	private JavaRDD<Vertex> vertices;
	private JavaRDD<Edge> edges;

	public Graph() {
		this.vertices = KeyPlayer.sc.emptyRDD();
		this.edges = KeyPlayer.sc.emptyRDD();
	}

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

	public long countVertex() {
		return vertices.count();
	}

	public long countEdge() {
		return edges.count();
	}

	public JavaRDD<Vertex> getVertices(){
		return this.vertices;
	}
	
	public JavaRDD<Edge> getEdges() {
		return this.edges;
	}
}
