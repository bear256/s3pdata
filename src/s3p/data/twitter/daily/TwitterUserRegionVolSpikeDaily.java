package s3p.data.twitter.daily;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.UserRegionVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.AnomalyDetectionUtils;
import s3p.data.utils.TableUtils;
import s3p.data.utils.anomalydetection.RequestBody;

public class TwitterUserRegionVolSpikeDaily {

	private static final String ENDPOINT = Endpoint.USERREGIONVOLSPIKE;

	private List<UserRegionVolSpike> list = new ArrayList<UserRegionVolSpike>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new TwitterUserRegionVolSpikeDaily().detect(listByPN, pn).save(tableName, dt, topic, pn);
		}
	}

	public TwitterUserRegionVolSpikeDaily detect(List<DocEntity> listByPN, String pn) {
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			UserRegionVolSpike userRegionVolSpike = JSON.parseObject(json, UserRegionVolSpike.class);
			list.add(userRegionVolSpike);
			long timeslot = userRegionVolSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String[] row = new String[2];
			row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			row[1] = "" + userRegionVolSpike.getVocinfluence().getUniqueuserregion();
			data.add(row);
			times.add(row[0]);
		}
		RequestBody requestBody = new RequestBody(data);
		List<String> spikeTimes = AnomalyDetectionUtils.listSpikeTime(requestBody);
		for (String spikeTime : spikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).getAttachedobject().setIsspike(true);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (UserRegionVolSpike userRegionVolSpike : list) {
			long timeslot = userRegionVolSpike.getAttachedobject().getTimeslot();
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(userRegionVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
