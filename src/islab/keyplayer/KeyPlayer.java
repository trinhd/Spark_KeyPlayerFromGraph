package islab.keyplayer;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class KeyPlayer {
	public static SparkConf conf = new SparkConf().setAppName("KeyPlayer").setMaster("local[*]");
	public static JavaSparkContext sc = new JavaSparkContext(conf);
	
	public static void main(String[] args) {
		System.out.println("--------------------------------->>>>>>Ngưỡng sức ảnh hưởng là: " + args[0]);
		Data.theta = new BigDecimal(args[0]);
		System.out.println("--------------------------------->>>>>>Ngưỡng số đỉnh chịu sức ảnh hưởng là: " + args[1]);
		Data.iNeed = Integer.parseInt(args[1]);
		
		// TODO Auto-generated method stub
		long lStart = System.currentTimeMillis();
		Graph g = new Graph();
		
		JavaRDD<String> dataFile = sc.textFile("./graph_data/graph_oneline_20160215.json");
		//System.out.println(dataFile.first());
		JavaRDD<Graph> gRDD = dataFile.map(new Data());
		//gRDD.saveAsTextFile("AAAAAA");
		g = gRDD.first();
		System.out.println("--------------------------------->>>>>>" + g.toString());
		List<List<String>> s = g.getAllPathBetweenTwoVertex("1", "18");
		System.out.println("--------------------------------->>>>>>Số đường đi: " + s.size());
		System.out.println("--------------------------------->>>>>>" + s);
		System.out.println("--------------------------------->>>>>>Sức ảnh hưởng gián tiếp giữa 2 đỉnh là: " + g.IndirectInfluenceOfVertexOnOtherVertex("1", "18"));
		
		Map<String, BigDecimal> all = g.getAllInfluenceOfVertices();
		
		System.out.println("--------------------------------->>>>>>Sức ảnh hưởng của tất cả các đỉnh:\n" + Arrays.toString(all.entrySet().toArray()));
		
		IndirectInfluence inif = g.getIndirectInfluence();
		System.out.println("--------------------------------->>>>>>Sức ảnh hưởng vượt ngưỡng của tất cả các đỉnh:\n" + inif.toString());
		
		String kp = g.getKeyPlayer();
		List<String> res = g.getSmallestGroup();
		long lEnd = System.currentTimeMillis();
		
		System.out.println("--------------------------------->>>>>>Thời gian tính toán là: " + (lEnd - lStart) + " ms");
		
		System.out.println("--------------------------------->>>>>>Key Player: " + kp.toString());
		
		System.out.println("--------------------------------->>>>>>Nhóm nhỏ nhất thỏa ngưỡng là: " + res);
	}
}
