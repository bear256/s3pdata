package s3p.data.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.storage.table.DocEntity;
import s3p.data.twitter.weekly.TwitterDailyInfluenceWeekly;
import s3p.data.twitter.weekly.TwitterDailyVolSpikeWeekly;
import s3p.data.twitter.weekly.TwitterImpactSummaryWeekly;
import s3p.data.twitter.weekly.TwitterInfluenceVolSpikeWeekly;
import s3p.data.twitter.weekly.TwitterKeywordsMentionedMostMappingWeekly;
import s3p.data.twitter.weekly.TwitterMentionedMostServiceListByUserVolWeekly;
import s3p.data.twitter.weekly.TwitterMentionedMostServiceListWeekly;
import s3p.data.twitter.weekly.TwitterMessageVolSpikeWeekly;
import s3p.data.twitter.weekly.TwitterPNDistributionWeekly;
import s3p.data.twitter.weekly.TwitterRegionDistributionWeekly;
import s3p.data.twitter.weekly.TwitterTopUserWeekly;
import s3p.data.twitter.weekly.TwitterUserRegionVolSpikeWeekly;
import s3p.data.twitter.weekly.TwitterUserVolSpikeWeekly;
import s3p.data.twitter.weekly.Weekly;
import s3p.data.utils.TableUtils;

public class TwitterWeekly {

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

	public static Map<String, List<DocEntity>> mergeByDaysOld(String daily, DateTime dt) {
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

	public static void runOld(String daily, DateTime dt, String weekly) {
		Map<String, List<DocEntity>> map = mergeByDaysOld(daily, dt);
		int num = 0, size = map.size();
		for (String endpointTopicPN : map.keySet()) {
			String[] fields = endpointTopicPN.split("-");
			String endpoint = fields[0];
			String topic = fields[1];
			String pn = fields[2];
			List<DocEntity> docs = map.get(endpointTopicPN);
			System.out.println((num++) + "/" + size + ": " + endpointTopicPN);
			// TopUser
			TwitterTopUserWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// PNDistribution
			TwitterPNDistributionWeekly.run(endpoint, docs, weekly, dt, topic);
			// RegionDistribution
			TwitterRegionDistributionWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// DailyInfluence
			TwitterDailyInfluenceWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// MessageVolSpike
			TwitterMessageVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// InfluenceVolSpike
			TwitterInfluenceVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// UserVolSpike
			TwitterUserVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// UserRegionVolSpike
			TwitterUserRegionVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// DailyVolSpike
			TwitterDailyVolSpikeWeekly.run(endpoint, docs, weekly, dt, topic);
			// MentionedMostServiceList
			TwitterMentionedMostServiceListWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// MentionedMostServiceListByUserVol
			TwitterMentionedMostServiceListByUserVolWeekly.run(endpoint, docs, weekly, dt, topic, pn);
			// KeywordsMentionedMostMapping
			TwitterKeywordsMentionedMostMappingWeekly.run(endpoint, docs, weekly, dt, topic, pn);
		}
	}

	public static Map<String, Weekly> mergeByDays(String daily, DateTime dt) {
		DateTime start = dt.minusDays(6);
		DateTime end = dt;
		DateTime cur = start;
		Map<String, Weekly> jobs = new HashMap<>();
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
						if (!jobs.containsKey(endpointTopicPN)) {
							Weekly job = null;
							switch (endpoint) {
							case Endpoint.TOPUSER:
								job = new TwitterTopUserWeekly();
								break;
							case Endpoint.PNDISTRIBUTION:
								job = new TwitterPNDistributionWeekly();
								break;
							case Endpoint.REGIONDISTRIBUTION:
								job = new TwitterRegionDistributionWeekly();
								break;
							case Endpoint.DAILYINFLUENCE:
								job = new TwitterDailyInfluenceWeekly();
								break;
							case Endpoint.MESSAGEVOLSPIKE:
								job = new TwitterMessageVolSpikeWeekly();
								break;
							case Endpoint.INFLUENCEVOLSPIKE:
								job = new TwitterInfluenceVolSpikeWeekly();
								break;
							case Endpoint.USERVOLSPIKE:
								job = new TwitterUserVolSpikeWeekly();
								break;
							case Endpoint.USERREGIONVOLSPIKE:
								job = new TwitterUserRegionVolSpikeWeekly();
								break;
							case Endpoint.DAILYVOLSPIKE:
								job = new TwitterDailyVolSpikeWeekly();
								break;
							case Endpoint.MENTIONEDMOSTSERVICELIST:
								job = new TwitterMentionedMostServiceListWeekly();
								break;
							case Endpoint.MENTIONEDMOSTSERVICELISTBYUSERVOL:
								job = new TwitterMentionedMostServiceListByUserVolWeekly();
								break;
							case Endpoint.KEYWORDSMENTIONEDMOSTMAPPING:
								job = new TwitterKeywordsMentionedMostMappingWeekly();
								break;
							}
							jobs.put(endpointTopicPN, job);
						}
						Weekly job = jobs.get(endpointTopicPN);
						job.merge(listByPN);
					}
				}
			}
			cur = cur.plusDays(1);
		}
		return jobs;
	}

	public static void run(String platform, String daily, DateTime dt, DateTime start, String weekly) {
		Set<String> topics = new HashSet<>();
		Map<String, Weekly> jobs = mergeByDays(daily, dt);
		for (String endpointTopicPN : jobs.keySet()) {
			String[] fields = endpointTopicPN.split("-");
			// String endpoint = fields[0];
			String topic = fields[1];
			topics.add(topic);
			String pn = fields[2];
			Weekly job = jobs.get(endpointTopicPN);
			job.save(weekly, dt, topic, pn);
		}
		if (start.numDaysFrom(dt) >= 13)
			for (String topic : topics) {
				new TwitterImpactSummaryWeekly().gen(platform, topic, dt).save(weekly, dt, topic, "ALL");
			}
	}

}
