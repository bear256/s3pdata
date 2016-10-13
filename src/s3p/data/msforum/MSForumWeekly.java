package s3p.data.msforum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import hirondelle.date4j.DateTime;
import s3p.data.msforum.weekly.MSForumDailyInfluenceWeekly;
import s3p.data.msforum.weekly.MSForumDailyVolSpikeWeekly;
import s3p.data.msforum.weekly.MSForumImpactSummaryWeekly;
import s3p.data.msforum.weekly.MSForumInfluenceVolSpikeWeekly;
import s3p.data.msforum.weekly.MSForumKeywordsMentionedMostMappingWeekly;
import s3p.data.msforum.weekly.MSForumMentionedMostServiceListByUserVolWeekly;
import s3p.data.msforum.weekly.MSForumMentionedMostServiceListWeekly;
import s3p.data.msforum.weekly.MSForumMessageVolSpikeWeekly;
import s3p.data.msforum.weekly.MSForumPNDistributionWeekly;
import s3p.data.msforum.weekly.MSForumTopUserWeekly;
import s3p.data.msforum.weekly.MSForumUserVolSpikeWeekly;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumWeekly {

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

	public static Map<String, List<DocEntity>> mergeByDays(String daily, DateTime dt) {
		DateTime start = dt.minusDays(6);
		DateTime end = dt;
		DateTime cur = start;
		Map<String, List<DocEntity>> total = new HashMap<>();
		while (cur.lteq(end)) {
			System.out.println("Weekly-" + cur);
			List<DocEntity> docs = listDocs(daily, cur.format("YYYY-MM-DD"));
			Map<String, List<DocEntity>> endpoints = groupByEndpoint(docs);
			for (String endpoint : endpoints.keySet()) {
				List<DocEntity> listByEndpoint = endpoints.get(endpoint);
				Map<String, List<DocEntity>> topics = groupByTopic(listByEndpoint);
				for (String topic : topics.keySet()) {
					List<DocEntity> listByTopic = topics.get(topic);
					Map<String, List<DocEntity>> pns = groupByPN(listByTopic);
					for (String pn : pns.keySet()) {
						List<DocEntity> listByPN = pns.get(pn);
						String endpointTopicPN = String.format("%s-%s-%s", endpoint, topic, pn);
						if (!total.containsKey(endpointTopicPN)) {
							List<DocEntity> list = new ArrayList<>();
							total.put(endpointTopicPN, list);
						}
						List<DocEntity> list = total.get(endpointTopicPN);
						list.addAll(listByPN);
					}
				}
			}
			cur = cur.plusDays(1);
		}
		return total;
	}

	public static void run(String platform, String daily, DateTime dt, DateTime start, String weekly) {
		Set<String> topics = new HashSet<>();
		Map<String, List<DocEntity>> map = mergeByDays(daily, dt);
		int num = 0, size = map.size();
		for (String endpointTopicPN : map.keySet()) {
			String[] fields = endpointTopicPN.split("-");
			String endpoint = fields[0];
			String topic = fields[1];
			topics.add(topic);
			String pn = fields[2];
			List<DocEntity> docs = map.get(endpointTopicPN);
			System.out.println((num++) + "/" + size + ": " + endpointTopicPN);
			// TopUser
			MSForumTopUserWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// PNDistribution
			MSForumPNDistributionWeekly.run(endpoint, docs, weekly, dt, topic);
			// RegionDistribution
			// DailyInfluence
			MSForumDailyInfluenceWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// MessageVolSpike
			MSForumMessageVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// InfluenceVolSpike
			MSForumInfluenceVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// UserVolSpike
			MSForumUserVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// UserRegionVolSpike
			// DailyVolSpike
			MSForumDailyVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic);
			// MentionedMostServiceList
			MSForumMentionedMostServiceListWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// MentionedMostServiceListByUserVol
			MSForumMentionedMostServiceListByUserVolWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// KeywordsMentionedMostMapping
			MSForumKeywordsMentionedMostMappingWeekly.run(endpoint, docs, weekly, dt, topic, pn);
		}
		if (start.numDaysFrom(dt) >= 13)
			for (String topic : topics) {
				new MSForumImpactSummaryWeekly().gen(platform, topic, dt).save(weekly, dt, topic, "ALL");
				;
			}
	}

}
