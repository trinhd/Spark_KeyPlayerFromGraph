package islab.keyplayer;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import scala.Tuple2;

public class Data implements Function<String, Graph> {
	public static BigDecimal theta = new BigDecimal("0.3");// ngưỡng sức ảnh hưởng
	public static int iK;//số phần tử trong tổ hợp, tăng dần để tìm cụm nhỏ nhất thỏa điều kiện.
	public static int iN;//số phần tử trong tập ban đầu cần xét, sẽ cắt giảm nếu các phần tử sau không thỏa.
	public static int iNeed = 15;//ngưỡng số đỉnh chịu sức ảnh hưởng vượt ngưỡng theta ở trên của một hoặc nhóm đỉnh trong đồ thị.
	public static boolean flagSorted = false;// cờ đánh dấu đã sắp xếp mảng hay chưa. 
	private FileReader fr = null;

	public void writeJsonFile(String sFileContent, String sPath) {
		try {
			Writer writer = new FileWriter(sPath);
			writer.write(sFileContent);
			writer.close();
		} catch (Exception e) {
			System.out.println("ERROR");
			System.out.println(
					"-----------------------------------------------------------------------------------------");
			System.out.println(e); // TODO: handle error
			System.out.println(
					"-----------------------------------------------------------------------------------------");
		}
	}
	
	public Graph createGraphFromJSONContent(String sContent) {
		//System.out.println(sContent);
		Graph g = new Graph();
		Gson gson = new Gson();

		try {
			// PARSE JSON file
			// Khoi tao Parser
			JsonParser parser = new JsonParser();
			// Parse reader thanh cac element
			JsonElement elements = parser.parse(sContent);
			// Chuyen cac element thanh cac object
			JsonObject objects = elements.getAsJsonObject();
			// Tach ra thanh mang cac Dinh (Vertex) de xu ly
			JsonArray jaVertices = objects.getAsJsonArray("vertices");
			// Tach lay cac Canh (Edge)
			JsonArray jaEdges = objects.getAsJsonArray("edges");

			// Tao mot dai dien cho kieu Map<String, BigDecimal> de deserialize
			// nhan dien
			/*Type typeSpreadCoefficient = new TypeToken<HashMap<String, BigDecimal>>() {
			}.getType();*/
			
			PairFunction<String, String, BigDecimal> pairfunc = new PairFunction<String, String, BigDecimal>() {
				
				@Override
				public Tuple2<String, BigDecimal> call(String arg0) throws Exception {
					// TODO Auto-generated method stub
					String[] str = arg0.split(":");
					//System.out.println("--------------------------------->>>>>>str[0]: " + str[0].substring(1, str[0].length() - 1));
					return new Tuple2<String, BigDecimal>(str[0].substring(1, str[0].length() - 1), new BigDecimal(str[1]));
				}
			};

			if (jaVertices != null) {
				for (JsonElement jeVertex : jaVertices) {
					JsonObject joVertex = jeVertex.getAsJsonObject();
					String sName = joVertex.get("Name").getAsString();
					JsonObject joSpreadCoefficient = joVertex.getAsJsonObject("SpreadCoefficient");
					Vertex vertex = null;
					JavaPairRDD<String, BigDecimal> mSpreadCoefficient = null;
					if (joSpreadCoefficient != null && !joSpreadCoefficient.isJsonNull()) {
						String strTemp = joSpreadCoefficient.toString();
						//System.out.println("--------------------------------->>>>>>joSpreadCoefficient: " + strTemp);
						strTemp = strTemp.substring(1, strTemp.length() - 1);
												
						JavaRDD<String> rddString = KeyPlayer.sc.parallelize(Arrays.asList(strTemp.split(",")));
						mSpreadCoefficient = rddString.mapToPair(pairfunc);
						mSpreadCoefficient.cache();
						
						if (mSpreadCoefficient != null && !mSpreadCoefficient.isEmpty()) {
							vertex = new Vertex(sName, mSpreadCoefficient);
						} else {
							vertex = new Vertex(sName);
						}
					} else {
						vertex = new Vertex(sName);
					}

					g.addVertex(vertex);
				}
			}

			if (jaEdges != null) {
				for (JsonElement jeEdge : jaEdges) {
					Edge edge = gson.fromJson(jeEdge, Edge.class);
					g.addEdge(edge);
				}
			}
			
		} catch (Exception e) {
			System.out.println("ERROR");
			System.out.println("DuyTri");
			System.out.println(
					"-----------------------------------------------------------------------------------------");
			System.out.println(e); // TODO: handle error
			//e.printStackTrace();
			System.out.println(
					"-----------------------------------------------------------------------------------------");
		}
		
		System.out.println("--------------------------------->>>>>>Số đỉnh đồ thị là: " + g.countVertex());
		return g;
	}

	@Override
	public Graph call(String arg0) throws Exception {
		// TODO Auto-generated method stub
		return createGraphFromJSONContent(arg0);
	}
}
