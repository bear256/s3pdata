package s3p.data.utils.anomalydetection;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import s3p.data.config.S3PDataConfig;
import s3p.data.utils.AnomalyDetectionUtils;

public class ResponseBody {

	public static void main(String[] args) {
		S3PDataConfig config = new S3PDataConfig();
		AnomalyDetectionUtils.init(config);
		String data = "[" + "[ \"9/21/2014 11:05:00 AM\", \"3\" ]," + " [ \"9/21/2014 11:10:00 AM\", \"9.09\" ],"
				+ " [ \"9/21/2014 11:15:00 AM\", \"0\" ]" + " ]";
		List<String[]> list = JSON.parseArray(data, String[].class);
		List<String> lst = new ArrayList<>();
		for (String[] array : list) {
			lst.add(array[0]);
		}
		// String json = AnomalyDetectionUtils.request(new RequestBody(list));
		String json = "{"
				+ "\"odata.metadata\":\"https://api.datamarket.azure.com/data.ashx/aml_labs/anomalydetection/v2/$metadata#AnomalyDetection.FrontEndService.Models.AnomalyDetectionResult\","
				+ "\"ADOutput\":{"
				+ "\"ColumnNames\":[\"Time\",\"Data\",\"TSpike\",\"ZSpike\",\"rpscore\",\"rpalert\",\"tscore\",\"talert\"],"
				+ "\"ColumnTypes\":[\"DateTime\",\"Double\",\"Double\",\"Double\",\"Double\",\"Int32\",\"Double\",\"Int32\"],"
				+ " \"Values\":["
				+ " [\"9/21/2014 11:10:00 AM\",\"9.09\",\"0\",\"0\",\"-1.07030497733224\",\"0\",\"-0.884548154298423\",\"0\"],"
				+ " [\"9/21/2014 11:15:00 AM\",\"0\",\"0\",\"0\",\"-1.05186237440962\",\"0\",\"-1.173800281031\",\"0\"]"
				+ " ]" + " }}";
		System.out.println(json);
		JSONObject jo = JSON.parseObject(json);
		JSONObject adOutput = jo.getJSONObject("ADOutput");
		JSONArray values = adOutput.getJSONArray("Values");
		int spikesvol = 0;
		for (int i = 0; i < values.size(); i++) {
			JSONArray value = values.getJSONArray(i);
			String[] v = new String[] { value.getString(0), value.getString(1) };
			System.out.println(JSON.toJSONString(v));
			int tspike = value.getInteger(2);
			int zspike = value.getInteger(3);
			System.out.println(lst.indexOf(value.getString(0)) + ":" + tspike + "-" + zspike);
			if (tspike == 1 || zspike == 1) {
				spikesvol++;
			}
		}
		System.out.println(spikesvol);
	}
}
