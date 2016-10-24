package s3p.data.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.microsoft.azure.storage.core.Base64;

import hirondelle.date4j.DateTime;
import s3p.data.config.S3PDataConfig;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.endpoint.volspikes.UserVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.anomalydetection.RequestBody;
import s3p.ws.config.Platform;

public class AnomalyDetectionUtils {

	private static String api;
	private static String accountId;
	private static String accountKey;
	private static String apiKey;

	public static void init(S3PDataConfig config) {
		api = config.get("anomalydetection.api");
		accountId = config.get("anomalydetection.accountId");
		accountKey = config.get("anomalydetection.accountKey");
		try {
			apiKey = Base64.encode((accountId + ":" + accountKey).getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public static String request(byte[] queryBytes) {
		String response = null;
		HttpURLConnection req;
		try {
			req = (HttpURLConnection) new URL(api).openConnection();
			req.addRequestProperty("ContentType", "application/json");
			req.addRequestProperty("Authorization", String.format("Basic %s", apiKey));
			req.setDoOutput(true);
			req.getOutputStream().write(queryBytes);
			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(req.getInputStream()));
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
			br.close();
			response = sb.toString();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return response;
	}

	public static String request(RequestBody reqBody) {
		byte[] queryBytes = JSON.toJSONString(reqBody).getBytes();
		return request(queryBytes);
	}

	public static List<String> listSpikeTime(RequestBody reqBody) {
		List<String> spikeTimes = new ArrayList<>();
		String json = request(reqBody);
//		System.out.println(json);
		JSONObject jo = JSON.parseObject(json);
		JSONObject adOutput = jo.getJSONObject("ADOutput");
		JSONArray values = adOutput.getJSONArray("Values");
		for (int i = 0; i < values.size(); i++) {
			JSONArray value = values.getJSONArray(i);
			String time = value.getString(0);
			int tspike = value.getInteger(2);
			int zspike = value.getInteger(3);
			if (tspike == 1 || zspike == 1) {
				spikeTimes.add(time);
			}
		}
		return spikeTimes;
	}
	
	public static void main(String[] args) {
		S3PDataConfig config = new S3PDataConfig();
		TableUtils.init(config);
		AnomalyDetectionUtils.init(config);
		String tableName = Platform.getHourlyTableName("msdn");
		String endpoint = Endpoint.MESSAGEVOLSPIKE;
		DateTime now = DateTime.now(TimeZone.getTimeZone("GMT+0"));
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for(DateTime cur = now.minusDays(30); cur.lteq(now); cur = cur.plusDays(1)) {
			String partitionKey = cur.format("YYYY-MM-DD");
			String rowKey1 = String.format("%s-0", endpoint);
			String rowKey2 = String.format("%s-z", endpoint);
			List<DocEntity> listByEndpoint = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
			for(DocEntity doc: listByEndpoint) {
				MessageVolSpike volSpike= JSON.parseObject(doc.getJson(), MessageVolSpike.class);
				long timeslot = volSpike.getAttachedobject().getTimeslot();
				DateTime dt = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
				int uniqueusers = volSpike.getVocinfluence().getVoctotalvol();
				String[] row = new String[2];
				row[0] = dt.format("M/D/YYYY h12:00:00 a", Locale.US);
				row[1] = "" + uniqueusers;
				data.add(row);
			}
		}
		System.out.println(JSON.toJSONString(data));
		RequestBody requestBody = new RequestBody(data);
		AnomalyDetectionUtils.listSpikeTime(requestBody);
	}
}
