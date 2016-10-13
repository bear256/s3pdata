package s3p.data.twitter.monthly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class TwitterVolSpikesMonthly {

	private static final String[] ENDPOINTS = new String[] { Endpoint.DAILYVOLSPIKE, Endpoint.USERVOLSPIKE,
			Endpoint.MESSAGEVOLSPIKE, Endpoint.INFLUENCEVOLSPIKE, Endpoint.USERREGIONVOLSPIKE };

	public static Map<String, List<DocEntity>> groupByEndpoint(List<DocEntity> docs) {
		Map<String, List<DocEntity>> map = new HashMap<>();
		for (DocEntity doc : docs) {
			String rowkey = doc.getRowKey();
			String[] fields = rowkey.split(":");
			String[] endpointTopicPN = fields[0].split("-");
			String endpoint = endpointTopicPN[0];
			if (!map.containsKey(endpoint)) {
				List<DocEntity> list = new ArrayList<>();
				map.put(endpoint, list);
			}
			List<DocEntity> list = map.get(endpoint);
			list.add(doc);
		}
		return map;
	}

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
						if(!map.containsKey(endpointTopicPN)) {
							List<DocEntity> list = new ArrayList<>();
							map.put(endpointTopicPN, list);
						}
						List<DocEntity> list = map.get(endpointTopicPN);
						list.addAll(listByPN);
					}
				}
			}
			for(String endpointTopicPN: map.keySet()) {
				List<DocEntity> list = map.get(endpointTopicPN);
				
			}
		}

	}

}
