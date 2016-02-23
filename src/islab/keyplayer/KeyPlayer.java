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
		System.out.println("Ngưỡng sức ảnh hưởng là: " + args[0]);
		Data.theta = KeyPlayer.sc.broadcast(new BigDecimal(args[0]));
		System.out.println("Ngưỡng số đỉnh chịu sức ảnh hưởng là: " + args[1]);
		Data.iNeed = KeyPlayer.sc.broadcast(Integer.parseInt(args[1]));
		String sInputPath = "./graph_data/graph_oneline.json";
		if (args[2].equals("-in")) {
			sInputPath = args[3];

			// TODO Auto-generated method stub
			long lStart = System.currentTimeMillis();
			Graph g = new Graph();

			Data data = new Data();
			g = data.createGraphFromJSONFile(sInputPath);
			final Broadcast<JavaRDD<Vertex>> bcVertices = sc.broadcast(sc.parallelize(g.getVertices()));
			final Broadcast<JavaRDD<Edge>> bcEdges = sc.broadcast(sc.parallelize(g.getEdges()));
			Utils u = new Utils(bcVertices, bcEdges);

			System.out.println("" + u.GraphToString());
			
			long lStart2 = System.currentTimeMillis();
			
			if (args[4].equals("-b1")) {
				lStart2 = System.currentTimeMillis();
				System.out.println("Sức ảnh hưởng gián tiếp của đỉnh " + args[5] + " lên đỉnh " + args[6] + " là: "
						+ u.IndirectInfluenceOfVertexOnOtherVertex(args[5], args[6]));
			}

			if (args[4].equals("-b2")) {
				lStart2 = System.currentTimeMillis();
				JavaPairRDD<String, BigDecimal> all = u.getAllInfluenceOfVertices();

				System.out.println("Sức ảnh hưởng của tất cả các đỉnh:");
				all.foreach(tuple -> {
					System.out.println("[ " + tuple._1 + " : " + tuple._2 + " ]");
				});
				
				System.out.println("Key Player là: ");
				System.out.println(all.first()._1 + ": " + all.first()._2.toString());
			}

			if (args[4].equals("-b3")) {
				lStart2 = System.currentTimeMillis();
				JavaPairRDD<String, List<String>> inif = u.getIndirectInfluence();
				System.out.println("Sức ảnh hưởng vượt ngưỡng của tất cả các đỉnh:");

				// In ra danh sách các đỉnh và các đỉnh chịu sức ảnh hưởng vượt
				// ngưỡng từ các đỉnh đó
				inif.foreach(f -> {
					System.out.print("\n" + f._1 + " : [");
					for (String string : f._2) {
						System.out.print(string + ", ");
					}
					System.out.print("]\n");
				});
				//

				String kp = u.getKeyPlayer();
				List<String> res = u.getSmallestGroup();
				System.out.println("Key Player: " + kp.toString());

				System.out.println("Nhóm nhỏ nhất thỏa ngưỡng là: " + res);
			}

			long lEnd = System.currentTimeMillis();

			System.out.println("Thời gian tính toán tổng cộng là: " + (lEnd - lStart) + " ms");
			
			System.out.println("Thời gian tính toán không tính thời gian tạo đồ thị là: " + (lEnd - lStart2) + " ms");
		}
	}
}
