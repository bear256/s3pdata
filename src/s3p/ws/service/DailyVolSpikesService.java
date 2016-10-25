package s3p.ws.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyvolspikes.DailyVolSpike;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class DailyVolSpikesService {
	
	private static final String ENDPOINT = Endpoint.DAILYVOLSPIKE;

	public String get(String platform, String topic, int days) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		String rowKey2 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		String json = "[]";
		if(!docs.isEmpty()) {
			DailyVolSpike[] volSpikes = JSON.parseObject(docs.get(0).getJson(), DailyVolSpike[].class);
			Map<String, Integer> map = getSpikes(platform, topic, days);
			for(DailyVolSpike volSpike: volSpikes) {
				long timeslot = volSpike.getDailytimeslot();
				DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
				String ymd = cur.format("YYYY-MM-DD");
				volSpike.setDailyspikevol(map.get(ymd));
			}
			json = JSON.toJSONString(volSpikes);
		}
		return json;
	}
	
	public Map<String, Integer> getSpikes(String platform, String topic, int days) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", Endpoint.MESSAGEVOLSPIKE, topic, "ALL");
		String rowKey2 = String.format("%s-%s-%s:z", Endpoint.MESSAGEVOLSPIKE, topic, "ALL");
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		Map<String, Integer> map = new HashMap<>();
		for (DocEntity doc : docs) {
			MessageVolSpike volSpike = JSON.parseObject(doc.getJson(), MessageVolSpike.class);
			long timeslot = volSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String ymd = cur.format("YYYY-MM-DD");
			int spikeNum = volSpike.getAttachedobject().isIsspike() ? 1:0;
			if (!map.containsKey(ymd)) {
				map.put(ymd, spikeNum);
			} else {
				map.put(ymd, spikeNum + map.get(ymd));
			}
		}
//		return JSON.toJSONString(map);
		return map;
	}

}
