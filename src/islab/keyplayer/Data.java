package islab.keyplayer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.broadcast.Broadcast;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

public class Data {//implements Function<String, Graph> {
	public static BigDecimal theta = new BigDecimal("0.3");// ngưỡng sức ảnh hưởng
	public static int iK;//số phần tử trong tổ hợp, tăng dần để tìm cụm nhỏ nhất thỏa điều kiện.
	public static int iN;//số phần tử trong tập ban đầu cần xét, sẽ cắt giảm nếu các phần tử sau không thỏa.
	public static int iNeed = 15;//ngưỡng số đỉnh chịu sức ảnh hưởng vượt ngưỡng theta ở trên của một hoặc nhóm đỉnh trong đồ thị.
	public static boolean flagSorted = false;// cờ đánh dấu đã sắp xếp mảng hay chưa. 
	private FileReader fr = null;

	private BufferedReader openJsonFile(String sPath) {
		try {
			fr = new FileReader(sPath);
			BufferedReader br = new BufferedReader(fr);
			return br;
		} catch (Exception e) {
			System.out.println("ERROR");
			System.out.println(
					"-----------------------------------------------------------------------------------------");
			System.out.println(e);
			System.out.println(
					"-----------------------------------------------------------------------------------------");
			return null;
		}
	}

	private void closeJsonFile() {
		try {
			if (fr != null) {
				fr.close();
			} else {
				throw new FileNotFoundException();
			}
		} catch (Exception e) {
			System.out.println("ERROR");
			System.out.println(
					"-----------------------------------------------------------------------------------------");
			System.out.println(e); // TODO: handle error
			System.out.println(
					"-----------------------------------------------------------------------------------------");
		}
	}
	
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
	
	public Graph createGraphFromJSONFile(String sPath) {
		Graph g = new Graph();
		Gson gson = new Gson();

		try {
			// PARSE JSON file
			// Khoi tao Parser
			JsonParser parser = new JsonParser();
			// Tao JsonReader tu BufferReader khi open json file
			JsonReader reader = new JsonReader(openJsonFile(sPath));
			// Parse reader thanh cac element
			JsonElement elements = parser.parse(reader);
			// Chuyen cac element thanh cac object
			JsonObject objects = elements.getAsJsonObject();
			// Tach ra thanh mang cac Dinh (Vertex) de xu ly
			JsonArray jaVertices = objects.getAsJsonArray("vertices");
			// Tach lay cac Canh (Edge)
			JsonArray jaEdges = objects.getAsJsonArray("edges");

			// Tao mot dai dien cho kieu Map<String, BigDecimal> de deserialize
			// nhan dien
			Type typeSpreadCoefficient = new TypeToken<HashMap<String, BigDecimal>>() {
			}.getType();

			if (jaVertices != null) {
				for (JsonElement jeVertex : jaVertices) {
					JsonObject joVertex = jeVertex.getAsJsonObject();
					String sName = joVertex.get("Name").getAsString();
					JsonObject joSpreadCoefficient = joVertex.getAsJsonObject("SpreadCoefficient");
					Vertex vertex = null;
					Map<String, BigDecimal> mSpreadCoefficient = null;
					if (joSpreadCoefficient != null) {
						mSpreadCoefficient = gson.fromJson(joSpreadCoefficient, typeSpreadCoefficient);
						if (mSpreadCoefficient != null) {
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

			closeJsonFile();
		} catch (Exception e) {
			System.out.println("ERROR");
			System.out.println(
					"-----------------------------------------------------------------------------------------");
			System.out.println(e); // TODO: handle error
			System.out.println(
					"-----------------------------------------------------------------------------------------");
		}
		System.out.println("--------------------------------->>>>>>Số đỉnh đồ thị là: " + g.countVertex());
		return g;
	}

	/*@Override
	public Graph call(String arg0) throws Exception {
		// TODO Auto-generated method stub
		return createGraphFromJSONContent(arg0);
	}*/
}
