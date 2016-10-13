package s3p.data.twitter.monthly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.dailyvolspikes.DailyVolSpike;
import s3p.data.endpoint.volspikes.InfluenceVolSpike;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.endpoint.volspikes.UserRegionVolSpike;
import s3p.data.endpoint.volspikes.UserVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.AnomalyDetectionUtils;
import s3p.data.utils.TableUtils;
import s3p.data.utils.anomalydetection.RequestBody;
import s3p.ws.config.Platform;

public class TwitterVolSpikesMonthly {

	private static final String[] ENDPOINTS = new String[] { Endpoint.DAILYVOLSPIKE, Endpoint.USERVOLSPIKE,
			Endpoint.MESSAGEVOLSPIKE, Endpoint.INFLUENCEVOLSPIKE, Endpoint.USERREGIONVOLSPIKE };

	public static Map<String, List<DocEntity>> groupByTopic(List<DocEntity> listByEndpoint) {
		Map<String, List<DocEntity>> map = new HashMap<>();
		for (DocEntity doc : listByEndpoint) {
			String rowkey = doc.getRowKey();
			String[] fields = rowkey.split(":");
			String[] endpointTopicPN = fields[0].split("-");
			String topic = endpointTopicPN[1];
			if (!map.containsKey(topic)) {
				List<DocEntity> list = new ArrayList<>();
				map.put(topic, list);
			}
			List<DocEntity> list = map.get(topic);
			list.add(doc);
		}
		return map;
	}

	public static Map<String, List<DocEntity>> groupByPN(List<DocEntity> listByTopic) {
		Map<String, List<DocEntity>> map = new HashMap<>();
		for (DocEntity doc : listByTopic) {
			String rowkey = doc.getRowKey();
			String[] fields = rowkey.split(":");
			String[] endpointTopicPN = fields[0].split("-");
			String pn = endpointTopicPN[2];
			if (!map.containsKey(pn)) {
				List<DocEntity> list = new ArrayList<>();
				map.put(pn, list);
			}
			List<DocEntity> list = map.get(pn);
			list.add(doc);
		}
		return map;
	}

	public static void detect4DailyVolSpike(List<DocEntity> listByPN) {
		List<String[]> neg = new ArrayList<>();
		List<String[]> neginfluence = new ArrayList<>();
		List<String[]> posi = new ArrayList<>();
		List<String[]> posiinfluence = new ArrayList<>();
		List<String[]> total = new ArrayList<>();
		List<String[]> totalinfluence = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			DailyVolSpike volSpike = JSON.parseObject(json, DailyVolSpike.class);
			long timeslot = volSpike.getDailytimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String time = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			neg.add(new String[] { time, "" + volSpike.getDailynegvol() });
			neginfluence.add(new String[] { time, "" + volSpike.getDailyneginfluencevol() });
			posi.add(new String[] { time, "" + volSpike.getDailyposivol() });
			posiinfluence.add(new String[] { time, "" + volSpike.getDailyposiinfluencevol() });
			total.add(new String[] { time, "" + volSpike.getDailytotalvol() });
			totalinfluence.add(new String[] { time, "" + volSpike.getDailytotalinfluencevol() });
			times.add(time);
		}
		// Neg
		RequestBody negRequestBody = new RequestBody(neg);
		List<String> negSpikeTimes = AnomalyDetectionUtils.listSpikeTime(negRequestBody);
		dailyVolSpike.setDailynegspike(negSpikeTimes.size());
		RequestBody neginfluenceRequestBody = new RequestBody(neginfluence);
		List<String> neginfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(neginfluenceRequestBody);
		dailyVolSpike.setDailynegspike(neginfluenceSpikeTimes.size());
		// Posi
		RequestBody posiRequestBody = new RequestBody(posi);
		List<String> posiSpikeTimes = AnomalyDetectionUtils.listSpikeTime(posiRequestBody);
		dailyVolSpike.setDailynegspike(posiSpikeTimes.size());
		RequestBody posiinfluenceRequestBody = new RequestBody(posiinfluence);
		List<String> posiinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(posiinfluenceRequestBody);
		dailyVolSpike.setDailynegspike(posiinfluenceSpikeTimes.size());
		// Total
		RequestBody totalRequestBody = new RequestBody(total);
		List<String> totalSpikeTimes = AnomalyDetectionUtils.listSpikeTime(totalRequestBody);
		dailyVolSpike.setDailynegspike(totalSpikeTimes.size());
		RequestBody totalinfluenceRequestBody = new RequestBody(totalinfluence);
		List<String> totalinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(totalinfluenceRequestBody);
		dailyVolSpike.setDailynegspike(totalinfluenceSpikeTimes.size());
	}

	public static void detect4UserVolSpike(List<DocEntity> listByPN) {
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			UserVolSpike userVolSpike = JSON.parseObject(json, UserVolSpike.class);
			long timeslot = userVolSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String[] row = new String[2];
			row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			row[1] = "" + userVolSpike.getVocinfluence().getUniqueusers();
			data.add(row);
			times.add(row[0]);
		}
		RequestBody requestBody = new RequestBody(data);
		List<String> spikeTimes = AnomalyDetectionUtils.listSpikeTime(requestBody);
		for (String spikeTime : spikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).getAttachedobject().setIsspike(true);
		}
	}

	public static void detect4MessageVolSpike(List<DocEntity> listByPN) {
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			MessageVolSpike messageVolSpike = JSON.parseObject(json, MessageVolSpike.class);
			long timeslot = messageVolSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String[] row = new String[2];
			row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			switch (pn) {
			case Sentiment.NEG:
				row[1] = "" + messageVolSpike.getVocinfluence().getNegativetotalvol();
				break;
			case Sentiment.POSI:
				row[1] = "" + messageVolSpike.getVocinfluence().getPositivetotalvol();
				break;
			default:
				row[1] = "" + messageVolSpike.getVocinfluence().getVoctotalvol();
				break;
			}
			data.add(row);
			times.add(row[0]);
		}
		RequestBody requestBody = new RequestBody(data);
		List<String> spikeTimes = AnomalyDetectionUtils.listSpikeTime(requestBody);
		for (String spikeTime : spikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).getAttachedobject().setIsspike(true);
		}
	}

	public static void detect4InfluenceVolSpike(List<DocEntity> listByPN) {
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			InfluenceVolSpike influenceVolSpike = JSON.parseObject(json, InfluenceVolSpike.class);
			long timeslot = influenceVolSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String[] row = new String[2];
			row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			switch (pn) {
			case Sentiment.NEG:
				row[1] = "" + influenceVolSpike.getVocinfluence().getNegativeinfluencedvol();
				break;
			case Sentiment.POSI:
				row[1] = "" + influenceVolSpike.getVocinfluence().getPositiveinfluencedvol();
				break;
			default:
				row[1] = "" + influenceVolSpike.getVocinfluence().getVocinfluencedvol();
				break;
			}
			data.add(row);
			times.add(row[0]);
		}
		RequestBody requestBody = new RequestBody(data);
		List<String> spikeTimes = AnomalyDetectionUtils.listSpikeTime(requestBody);
		for (String spikeTime : spikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).getAttachedobject().setIsspike(true);
		}
	}

	public static void detect4UserRegionVolSpike(List<DocEntity> listByPN) {
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			UserRegionVolSpike userRegionVolSpike = JSON.parseObject(json, UserRegionVolSpike.class);
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
	}

	public static void main(String[] args) {
		String platform = "twitter";
		String tableName = Platform.getHourlyTableName(platform);
		System.out.println(tableName);
		DateTime now = DateTime.now(TimeZone.getTimeZone("GMT+0"));
		DateTime start = now.minusDays(30);
		DateTime end = now;
		for (String endpoint : ENDPOINTS) {
			System.out.println(endpoint);
			Map<String, List<DocEntity>> map = new HashMap<>();
			for (DateTime cur = start; cur.lteq(end); cur = cur.plusDays(1)) {
				String partitionKey = cur.format("YYYY-MM-DD");
				String rowKey1 = String.format("%s-0", endpoint);
				String rowKey2 = String.format("%s-z", endpoint);
				List<DocEntity> listByEndpoint = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
				Map<String, List<DocEntity>> topics = groupByTopic(listByEndpoint);
				for (String topic : topics.keySet()) {
					List<DocEntity> listByTopic = topics.get(topic);
					Map<String, List<DocEntity>> pns = groupByPN(listByTopic);
					for (String pn : pns.keySet()) {
						List<DocEntity> listByPN = pns.get(pn);
						String endpointTopicPN = String.format("%s-%s-%s", endpoint, topic, pn);
						if (!map.containsKey(endpointTopicPN)) {
							List<DocEntity> list = new ArrayList<>();
							map.put(endpointTopicPN, list);
						}
						List<DocEntity> list = map.get(endpointTopicPN);
						list.addAll(listByPN);
					}
				}
			}
			for (String endpointTopicPN : map.keySet()) {
				List<DocEntity> listByPN = map.get(endpointTopicPN);
				switch (endpoint) {
				case Endpoint.DAILYVOLSPIKE:
					detect4DailyVolSpike(listByPN);
					break;
				case Endpoint.USERVOLSPIKE:
					detect4UserVolSpike(listByPN);
					break;
				case Endpoint.MESSAGEVOLSPIKE:
					detect4MessageVolSpike(listByPN);
					break;
				case Endpoint.INFLUENCEVOLSPIKE:
					detect4InfluenceVolSpike(listByPN);
					break;
				case Endpoint.USERREGIONVOLSPIKE:
					detect4UserRegionVolSpike(listByPN);
					break;
				}
			}
		}

	}

}
