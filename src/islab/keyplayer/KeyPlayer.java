package islab.keyplayer;

import java.math.BigDecimal;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class KeyPlayer {
	public static SparkConf conf = new SparkConf().setAppName("KeyPlayer");
	public static JavaSparkContext sc = new JavaSparkContext(conf);
	
	public static void main(String[] args) {
		System.out.println("--------------------------------->>>>>>Ngưỡng sức ảnh hưởng là: " + args[0]);
		Data.theta = KeyPlayer.sc.broadcast(new BigDecimal(args[0]));
		System.out.println("--------------------------------->>>>>>Ngưỡng số đỉnh chịu sức ảnh hưởng là: " + args[1]);
		Data.iNeed = KeyPlayer.sc.broadcast(Integer.parseInt(args[1]));
		String sInputPath = "./graph_data/graph_oneline.json";
		if (args[2].equals("-in")){
			sInputPath = args[3];
		}
		
		// TODO Auto-generated method stub
		long lStart = System.currentTimeMillis();
		Graph g = new Graph();
		
		/*JavaRDD<String> dataFile = sc.textFile("./graph_data/graph_oneline.json");
		//dataFile.cache();
		JavaRDD<Graph> gRDD = dataFile.map(new Data());
		//gRDD.saveAsTextFile("AAAAAA");
		gRDD.cache();
		g = gRDD.first();*/
		
		Data data = new Data();
		g = data.createGraphFromJSONFile(sInputPath);
		final Broadcast<JavaRDD<Vertex>> bcVertices = sc.broadcast(sc.parallelize(g.getVertices()));
		final Broadcast<JavaRDD<Edge>> bcEdges = sc.broadcast(sc.parallelize(g.getEdges()));
		Utils u = new Utils(bcVertices, bcEdges);
		//JavaRDD<Vertex> vertices = g.getVertices();
		//JavaRDD<Edge> edges = g.getEdges();
		
		System.out.println("--------------------------------->>>>>>" + u.GraphToString());
		/*
		 * JavaRDD<List<String>> s = u.getAllPathBetweenTwoVertex("1", "18");
		s.cache();
		System.out.println("--------------------------------->>>>>>Số đường đi: " + s.count());
		System.out.println("--------------------------------->>>>>>");// + s.toString());
		s.foreach(path -> {
			System.out.println(path);
		});
		
		System.out.println("--------------------------------->>>>>>Sức ảnh hưởng gián tiếp giữa 2 đỉnh là: " + u.IndirectInfluenceOfVertexOnOtherVertex("1", "18"));
		*/
		JavaPairRDD<String, BigDecimal> all = u.getAllInfluenceOfVertices();
		
		System.out.println("--------------------------------->>>>>>Sức ảnh hưởng của tất cả các đỉnh:");// + all.toString());
		all.foreach(tuple -> {
			System.out.println("[ " + tuple._1 + " : " + tuple._2 + " ]");
		});
		
		JavaPairRDD<String, List<String>> inif = u.getIndirectInfluence();
		System.out.println("--------------------------------->>>>>>Sức ảnh hưởng vượt ngưỡng của tất cả các đỉnh:");// + inif.toString());
		
		//In ra danh sách các đỉnh và các đỉnh chịu sức ảnh hưởng vượt ngưỡng từ các đỉnh đó
		inif.foreach(f -> 
		{
			System.out.print("\n" + f._1 + " : [");
			for (String string : f._2) {
				System.out.print(string + ", ");
			}
			System.out.print("]\n");
		});
		//////////
		
		String kp = u.getKeyPlayer();
		List<String> res = u.getSmallestGroup();
		long lEnd = System.currentTimeMillis();
		
		System.out.println("--------------------------------->>>>>>Thời gian tính toán là: " + (lEnd - lStart) + " ms");
		
		System.out.println("--------------------------------->>>>>>Key Player: " + kp.toString());
		
		System.out.println("--------------------------------->>>>>>Nhóm nhỏ nhất thỏa ngưỡng là: " + res);
	}
}
