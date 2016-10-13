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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.microsoft.azure.storage.core.Base64;

import s3p.data.config.S3PDataConfig;
import s3p.data.utils.anomalydetection.RequestBody;

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
}
