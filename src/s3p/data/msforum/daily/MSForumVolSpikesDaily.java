package s3p.data.msforum.daily;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.config.S3PDataConfig;
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

public class MSForumVolSpikesDaily {

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

	public static void detect4DailyVolSpike(List<DocEntity> listByPN, String platform, String endpoint, String topic,
			String pn) {
		String tableName = Platform.getDailyTableName(platform);
		List<DailyVolSpike> list = new ArrayList<DailyVolSpike>();
		List<String[]> undef = new ArrayList<>();
		List<String[]> undefinfluence = new ArrayList<>();
		List<String[]> neg = new ArrayList<>();
		List<String[]> neginfluence = new ArrayList<>();
		List<String[]> neu = new ArrayList<>();
		List<String[]> neuinfluence = new ArrayList<>();
		List<String[]> posi = new ArrayList<>();
		List<String[]> posiinfluence = new ArrayList<>();
		List<String[]> total = new ArrayList<>();
		List<String[]> totalinfluence = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			DailyVolSpike volSpike = JSON.parseObject(json, DailyVolSpike.class);
			list.add(volSpike);
			long timeslot = volSpike.getDailytimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			if (cur.format("YYYY-MM-DD").equals("2016-10-14"))
				System.out.println(JSON.toJSONString(volSpike));
			String time = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			undef.add(new String[] { time, "" + volSpike.getDailyundefvol() });
			undefinfluence.add(new String[] { time, "" + volSpike.getDailyundefinfluencevol() });
			neg.add(new String[] { time, "" + volSpike.getDailynegvol() });
			neginfluence.add(new String[] { time, "" + volSpike.getDailyneginfluencevol() });
			neu.add(new String[] { time, "" + volSpike.getDailyneuvol() });
			neuinfluence.add(new String[] { time, "" + volSpike.getDailyneuinfluencevol() });
			posi.add(new String[] { time, "" + volSpike.getDailyposivol() });
			posiinfluence.add(new String[] { time, "" + volSpike.getDailyposiinfluencevol() });
			total.add(new String[] { time, "" + volSpike.getDailytotalvol() });
			totalinfluence.add(new String[] { time, "" + volSpike.getDailytotalinfluencevol() });
			times.add(time);
		}
		// Undef
		RequestBody undefRequestBody = new RequestBody(undef);
		List<String> undefSpikeTimes = AnomalyDetectionUtils.listSpikeTime(undefRequestBody);
		for (String spikeTime : undefSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyundefspike(1);
		}
		RequestBody undefinfluenceRequestBody = new RequestBody(undefinfluence);
		List<String> undefinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(undefinfluenceRequestBody);
		for (String spikeTime : undefinfluenceSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyundefinfluencespike(1);
		}
		// Neg
		RequestBody negRequestBody = new RequestBody(neg);
		List<String> negSpikeTimes = AnomalyDetectionUtils.listSpikeTime(negRequestBody);
		for (String spikeTime : negSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailynegspike(1);
		}
		RequestBody neginfluenceRequestBody = new RequestBody(neginfluence);
		List<String> neginfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(neginfluenceRequestBody);
		for (String spikeTime : neginfluenceSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyneginfluencespike(1);
		}
		// Neu
		RequestBody neuRequestBody = new RequestBody(neu);
		List<String> neuSpikeTimes = AnomalyDetectionUtils.listSpikeTime(neuRequestBody);
		for (String spikeTime : neuSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyneuspike(1);
		}
		RequestBody neuinfluenceRequestBody = new RequestBody(neuinfluence);
		List<String> neuinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(neuinfluenceRequestBody);
		for (String spikeTime : neuinfluenceSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyneuinfluencespike(1);
		}
		// Posi
		RequestBody posiRequestBody = new RequestBody(posi);
		List<String> posiSpikeTimes = AnomalyDetectionUtils.listSpikeTime(posiRequestBody);
		for (String spikeTime : posiSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyposispike(1);
		}
		RequestBody posiinfluenceRequestBody = new RequestBody(posiinfluence);
		List<String> posiinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(posiinfluenceRequestBody);
		for (String spikeTime : posiinfluenceSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyposiinfluencespike(1);
		}
		// Total
		RequestBody totalRequestBody = new RequestBody(total);
		List<String> totalSpikeTimes = AnomalyDetectionUtils.listSpikeTime(totalRequestBody);
		for (String spikeTime : totalSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyspikevol(1);
		}
		RequestBody totalinfluenceRequestBody = new RequestBody(totalinfluence);
		List<String> totalinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(totalinfluenceRequestBody);
		for (String spikeTime : totalinfluenceSpikeTimes) {
			int idx = times.indexOf(spikeTime);
			list.get(idx).setDailyinfluencespikevol(1);
		}
		Map<String, DailyVolSpike> map = new HashMap<>();
		for (DailyVolSpike volSpike : list) {
			long timeslot = volSpike.getDailytimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String ymd = cur.format("YYYY-MM-DD");
			if (ymd.equals("2016-10-14"))
				System.out.println(JSON.toJSONString(volSpike));
			if (!map.containsKey(ymd)) {
				map.put(ymd, volSpike);
			} else {
				map.get(ymd).merge(volSpike);
			}
		}
		for (String partitionKey : map.keySet()) {
			DailyVolSpike dailyVolSpike = map.get(partitionKey);
			String rowKey = String.format("%s-%s-%s", endpoint, topic.toUpperCase(), "ALL");
			String json = JSON.toJSONString(dailyVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}

	public static void detect4UserVolSpike(List<DocEntity> listByPN, String platform, String endpoint, String topic,
			String pn) {
		String tableName = Platform.getDailyTableName(platform);
		List<UserVolSpike> list = new ArrayList<UserVolSpike>();
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			UserVolSpike userVolSpike = JSON.parseObject(json, UserVolSpike.class);
			list.add(userVolSpike);
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
		for (UserVolSpike userVolSpike : list) {
			long timeslot = userVolSpike.getAttachedobject().getTimeslot();
			DateTime dt = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", endpoint, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(userVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}

	public static void detect4MessageVolSpike(List<DocEntity> listByPN, String platform, String endpoint, String topic,
			String pn) {
		String tableName = Platform.getDailyTableName(platform);
		List<MessageVolSpike> list = new ArrayList<MessageVolSpike>();
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			MessageVolSpike messageVolSpike = JSON.parseObject(json, MessageVolSpike.class);
			list.add(messageVolSpike);
			long timeslot = messageVolSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String[] row = new String[2];
			row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			switch (pn) {
			case Sentiment.UNDEF:
				row[1] = "" + messageVolSpike.getVocinfluence().getUndefinedtotalvol();
				break;
			case Sentiment.NEG:
				row[1] = "" + messageVolSpike.getVocinfluence().getNegativetotalvol();
				break;
			case Sentiment.NEU:
				row[1] = "" + messageVolSpike.getVocinfluence().getNeutraltotalvol();
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
		for (MessageVolSpike messageVolSpike : list) {
			long timeslot = messageVolSpike.getAttachedobject().getTimeslot();
			DateTime dt = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", endpoint, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(messageVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);

		}
	}

	public static void detect4InfluenceVolSpike(List<DocEntity> listByPN, String platform, String endpoint,
			String topic, String pn) {
		String tableName = Platform.getDailyTableName(platform);
		List<InfluenceVolSpike> list = new ArrayList<InfluenceVolSpike>();
		List<String[]> data = new ArrayList<>();
		List<String> times = new ArrayList<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			InfluenceVolSpike influenceVolSpike = JSON.parseObject(json, InfluenceVolSpike.class);
			list.add(influenceVolSpike);
			long timeslot = influenceVolSpike.getAttachedobject().getTimeslot();
			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String[] row = new String[2];
			row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
			switch (pn) {
			case Sentiment.UNDEF:
				row[1] = "" + influenceVolSpike.getVocinfluence().getUndefinedinfluencedvol();
				break;
			case Sentiment.NEG:
				row[1] = "" + influenceVolSpike.getVocinfluence().getNegativeinfluencedvol();
				break;
			case Sentiment.NEU:
				row[1] = "" + influenceVolSpike.getVocinfluence().getNeutralinfluencedvol();
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
		for (InfluenceVolSpike influenceVolSpike : list) {
			long timeslot = influenceVolSpike.getAttachedobject().getTimeslot();
			DateTime dt = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", endpoint, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(influenceVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}

	public static void detect4UserRegionVolSpike(List<DocEntity> listByPN, String platform, String endpoint,
			String topic, String pn) {
		String tableName = Platform.getDailyTableName(platform);
		List<UserRegionVolSpike> list = new ArrayList<UserRegionVolSpike>();
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
		for (UserRegionVolSpike userRegionVolSpike : list) {
			long timeslot = userRegionVolSpike.getAttachedobject().getTimeslot();
			DateTime dt = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", endpoint, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(userRegionVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}

	public static void run(String platform, DateTime now) {
		String tableName = Platform.getHourlyTableName(platform);
		System.out.println(tableName);
		// DateTime now = DateTime.now(TimeZone.getTimeZone("GMT+0"));
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
				String[] fields = endpointTopicPN.split("-");
				String topic = fields[1];
				String pn = fields[2];
				List<DocEntity> listByPN = map.get(endpointTopicPN);
				switch (endpoint) {
				case Endpoint.DAILYVOLSPIKE:
					detect4DailyVolSpike(listByPN, platform, endpoint, topic, pn);
					break;
				case Endpoint.USERVOLSPIKE:
					detect4UserVolSpike(listByPN, platform, endpoint, topic, pn);
					break;
				case Endpoint.MESSAGEVOLSPIKE:
					detect4MessageVolSpike(listByPN, platform, endpoint, topic, pn);
					break;
				case Endpoint.INFLUENCEVOLSPIKE:
					detect4InfluenceVolSpike(listByPN, platform, endpoint, topic, pn);
					break;
				case Endpoint.USERREGIONVOLSPIKE:
					detect4UserRegionVolSpike(listByPN, platform, endpoint, topic, pn);
					break;
				}
			}
		}

	}

	public static void main(String[] args) {
		S3PDataConfig config = new S3PDataConfig();
		TableUtils.init(config);
		AnomalyDetectionUtils.init(config);
		run("msdn", new DateTime("2016-10-14"));
	}

}
