package s3p.data.msforum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hirondelle.date4j.DateTime;
import s3p.data.msforum.daily.MSForumDailyInfluenceDaily;
import s3p.data.msforum.daily.MSForumDailyVolSpikeDaily;
import s3p.data.msforum.daily.MSForumInfluenceVolSpikeDaily;
import s3p.data.msforum.daily.MSForumKeywordsMentionedMostMappingDaily;
import s3p.data.msforum.daily.MSForumMentionedMostServiceListByUserVolDaily;
import s3p.data.msforum.daily.MSForumMentionedMostServiceListDaily;
import s3p.data.msforum.daily.MSForumMessageVolSpikeDaily;
import s3p.data.msforum.daily.MSForumPNDistributionDaily;
import s3p.data.msforum.daily.MSForumTopUserDaily;
import s3p.data.msforum.daily.MSForumUserVolSpikeDaily;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumDaily {

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
				MSForumPNDistributionDaily.run(endpoint, listByTopic, daily, cur, topic);
				// DailyVolSpike
				MSForumDailyVolSpikeDaily.run(endpoint, listByTopic, daily, cur, topic);
				Map<String, List<DocEntity>> pns = groupByPN(listByTopic);
				for (String pn : pns.keySet()) {
					List<DocEntity> listByPN = pns.get(pn);
					// TopUser
					MSForumTopUserDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// RegionDistribution
					// DailyInfluence
					MSForumDailyInfluenceDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// MessageVolSpike
					MSForumMessageVolSpikeDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// InfluenceVolSpike
					MSForumInfluenceVolSpikeDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// UserVolSpike
					MSForumUserVolSpikeDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// UserRegionVolSpike
					// MentionedMostServiceList
					MSForumMentionedMostServiceListDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// MentionedMostServiceListByUserVol
					MSForumMentionedMostServiceListByUserVolDaily.run(endpoint, listByPN, daily, cur, topic, pn);
					// KeywordsMentionedMostMapping
					MSForumKeywordsMentionedMostMappingDaily.run(endpoint, listByPN, daily, cur, topic, pn);
				}
			}
		}
	}

}
