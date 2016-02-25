package islab.keyplayer;

import java.math.BigDecimal;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class KeyPlayer {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("KeyPlayerSpark").setMaster("spark://PTNHTTT10:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String sInputPath = "./graph_data/graph_oneline.json";
		if (args[0].equals("-in")) {
			sInputPath = args[1];

			// TODO Auto-generated method stub
			long lStart = System.currentTimeMillis();
			Graph g = new Graph();

			Data data = new Data();
			g = data.createGraphFromJSONFile(sInputPath);
			List<Vertex> vertices = g.getVertices();
			List<Edge> edges = g.getEdges();
			
			Utils u = new Utils(sc);
			
			System.out.println("" + u.GraphToString(vertices, edges));
			
			long lStart2 = System.currentTimeMillis();
			
			if (args[2].equals("-b1")) {
				lStart2 = System.currentTimeMillis();
				System.out.println("Sức ảnh hưởng gián tiếp của đỉnh " + args[3] + " lên đỉnh " + args[4] + " là: "
						+ u.IndirectInfluenceOfVertexOnOtherVertex(vertices, edges, args[3], args[4]));
			}

			if (args[2].equals("-b2")) {
				lStart2 = System.currentTimeMillis();
				JavaPairRDD<String, BigDecimal> all = u.getAllInfluenceOfVertices(vertices, edges);
				all.cache();

				System.out.println("Sức ảnh hưởng của tất cả các đỉnh:");
				List<Tuple2<String, BigDecimal>> listAll = all.collect();
				for (Tuple2<String, BigDecimal> tuple : listAll) {
					System.out.println("[ " + tuple._1 + " : " + tuple._2.toString() + " ]");
				}
				
				System.out.println("Key Player là: ");
				Tuple2<String, BigDecimal> kp = all.first();
				System.out.println(kp._1 + ": " + kp._2.toString());
			}

			if (args[2].equals("-b3")) {
				lStart2 = System.currentTimeMillis();
				
				System.out.println("Ngưỡng sức ảnh hưởng là: " + args[3]);
				Data.theta = new BigDecimal(args[3]);
				System.out.println("Ngưỡng số đỉnh chịu sức ảnh hưởng là: " + args[4]);
				Data.iNeed = Integer.parseInt(args[4]);
				JavaPairRDD<String, List<String>> inif = u.getIndirectInfluence(vertices, edges);
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

				String kp = u.getTheMostOverThresholdVertexName(vertices, edges);
				List<String> res = u.getSmallestGroup(vertices, edges);
				System.out.println("Đỉnh có sức ảnh hưỡng vượt ngưỡng đến các đỉnh khác nhiều nhất là: " + kp.toString());

				System.out.println("Nhóm nhỏ nhất thỏa ngưỡng là: " + res);
			}

			long lEnd = System.currentTimeMillis();

			System.out.println("Thời gian tính toán tổng cộng là: " + (lEnd - lStart) + " ms");
			
			System.out.println("Thời gian tính toán không tính thời gian tạo đồ thị là: " + (lEnd - lStart2) + " ms");
		}
	}
}
