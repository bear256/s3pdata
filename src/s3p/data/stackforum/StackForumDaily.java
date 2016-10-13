package s3p.data.stackforum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hirondelle.date4j.DateTime;
import s3p.data.stackforum.daily.StackForumDailyInfluenceDaily;
import s3p.data.stackforum.daily.StackForumDailyVolSpikeDaily;
import s3p.data.stackforum.daily.StackForumInfluenceVolSpikeDaily;
import s3p.data.stackforum.daily.StackForumKeywordsMentionedMostMappingDaily;
import s3p.data.stackforum.daily.StackForumMentionedMostServiceListByUserVolDaily;
import s3p.data.stackforum.daily.StackForumMentionedMostServiceListDaily;
import s3p.data.stackforum.daily.StackForumMessageVolSpikeDaily;
import s3p.data.stackforum.daily.StackForumPNDistributionDaily;
import s3p.data.stackforum.daily.StackForumTopUserDaily;
import s3p.data.stackforum.daily.StackForumUserVolSpikeDaily;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumDaily {

	public static List<DocEntity> listDocs(String tableName, String partitionKey) {
		return TableUtils.readDocs(tableName, partitionKey);
	}

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

	public static void run(String hourly, DateTime cur, String daily) {
		List<DocEntity> docs = listDocs(hourly, cur.format("YYYY-MM-DD"));
		Map<String, List<DocEntity>> endpoints = groupByEndpoint(docs);
		for (String endpoint : endpoints.keySet()) {
			List<DocEntity> listByEndpoint = endpoints.get(endpoint);
			Map<String, List<DocEntity>> topics = groupByTopic(listByEndpoint);
			for (String topic : topics.keySet()) {
				List<DocEntity> listByTopic = topics.get(topic);
				// PNDistribution
				StackForumPNDistributionDaily.run(endpoint, listByTopic, daily, cur, topic);
				// DailyVolSpike
				StackForumDailyVolSpikeDaily.run(endpoint, listByTopic, daily, cur, topic);
				Map<String, List<DocEntity>> pns = groupByPN(listByTopic);
				for (String pn : pns.keySet()) {
					List<DocEntity> listByPN = pns.get(pn);
					// TopUser
					StackForumTopUserDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// RegionDistribution
					// DailyInfluence
					StackForumDailyInfluenceDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// MessageVolSpike
					StackForumMessageVolSpikeDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// InfluenceVolSpike
					StackForumInfluenceVolSpikeDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// UserVolSpike
					StackForumUserVolSpikeDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// UserRegionVolSpike
					// MentionedMostServiceList
					StackForumMentionedMostServiceListDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// MentionedMostServiceListByUserVol
					StackForumMentionedMostServiceListByUserVolDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// KeywordsMentionedMostMapping
					StackForumKeywordsMentionedMostMappingDaily.run(endpoint, listByPN, daily, cur, topic, pn);
				}
			}
		}
	}

}
