package s3p.data.stackforum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.stackforum.hourly.StackForumKeywordsMentionedMostMappingHourly;
import s3p.data.stackforum.hourly.StackForumDailyInfluenceHourly;
import s3p.data.stackforum.hourly.StackForumDailyVolSpikeHourly;
import s3p.data.stackforum.hourly.StackForumInfluenceVolSpikeHourly;
import s3p.data.stackforum.hourly.StackForumMentionedMostServiceListByUserVolHourly;
import s3p.data.stackforum.hourly.StackForumMentionedMostServiceListHourly;
import s3p.data.stackforum.hourly.StackForumMessageVolSpikeHourly;
import s3p.data.stackforum.hourly.StackForumPNDistributionHourly;
import s3p.data.stackforum.hourly.StackForumTopUserHourly;
import s3p.data.stackforum.hourly.StackForumUserVolSpikeHourly;
import s3p.data.stackforum.hourly.StackForumVoCDetailHourly;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumHourly {

	public static List<DocEntity> listDocs(String tableName, String partitionKey) {
		return TableUtils.readDocs(tableName, partitionKey);
	}

	public static Map<String, List<StackForum>> groupByTopic(List<DocEntity> docs) {
		Map<String, List<StackForum>> map = new HashMap<>();
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			StackForum stackForum = JSON.parseObject(json, StackForum.class);
			List<String> topics = stackForum.topics();
			for (String topic : topics) {
				if (!map.containsKey(topic)) {
					List<StackForum> list = new ArrayList<>();
					map.put(topic, list);
				}
				List<StackForum> list = map.get(topic);
				list.add(stackForum);
			}
		}
		return map;
	}

	public static Map<String, List<StackForum>> groupByPN(List<StackForum> listByTopic) {
		Map<String, List<StackForum>> map = new HashMap<>();
		for (StackForum stackForum : listByTopic) {
			int sentimentScore = stackForum.getSentimentscore();
			String sentiment = Sentiment.get(sentimentScore);
			if (!map.containsKey(sentiment)) {
				List<StackForum> listByPN = new ArrayList<>();
				map.put(sentiment, listByPN);
			}
			List<StackForum> listByPN = map.get(sentiment);
			listByPN.add(stackForum);
		}
		if (map.get(Sentiment.ALL) == null) {
			List<StackForum> listByPN = new ArrayList<>();
			map.put(Sentiment.ALL, listByPN);
		}
		if(map.get(Sentiment.UNDEF) != null) 
			map.get(Sentiment.ALL).addAll(map.get(Sentiment.UNDEF));
		if (map.get(Sentiment.NEG) != null)
			map.get(Sentiment.ALL).addAll(map.get(Sentiment.NEG));
		if (map.get(Sentiment.NEU) != null)
			map.get(Sentiment.ALL).addAll(map.get(Sentiment.NEU));
		if (map.get(Sentiment.POSI) != null)
			map.get(Sentiment.ALL).addAll(map.get(Sentiment.POSI));
		return map;
	}

	public static void run(String raw, DateTime cur, String hourly) {
		List<DocEntity> docs = listDocs(raw, cur.format("YYYY-MM-DD hh:00:00"));
		Map<String, List<StackForum>> topics = groupByTopic(docs);
		for (String topic : topics.keySet()) {
			List<StackForum> listByTopic = topics.get(topic);
			// PNDistribution
			new StackForumPNDistributionHourly().totalVocinfluence(listByTopic).save(hourly, cur, topic);
			// DailyVolSpike
			new StackForumDailyVolSpikeHourly().total(listByTopic).save(hourly, cur, topic);
			Map<String, List<StackForum>> pns = groupByPN(listByTopic);
			for (String pn : pns.keySet()) {
				List<StackForum> listByPN = pns.get(pn);
				// TopUser
				new StackForumTopUserHourly().groupByUserId(listByPN).save(hourly, cur, topic, pn);
				// RegionDistribution
				// DailyInfluence
				new StackForumDailyInfluenceHourly().totalVocinfluence(listByPN).save(hourly, cur, topic, pn);
				// MessageVolSpike
				new StackForumMessageVolSpikeHourly().total(listByPN).save(hourly, cur, topic, pn);
				// InfluenceVolSpike
				new StackForumInfluenceVolSpikeHourly().total(listByPN).save(hourly, cur, topic, pn);
				// UserVolSpike
				new StackForumUserVolSpikeHourly().groupByUserId(listByPN).save(hourly, cur, topic, pn);
				// UserRegionVolSpike
				// MentionedMostServiceList
				new StackForumMentionedMostServiceListHourly().groupByService(topic, listByPN).save(hourly, cur, topic,
						pn);
				// MentionedMostServiceListByUserVol
				new StackForumMentionedMostServiceListByUserVolHourly().groupByService(topic, listByPN).save(hourly,
						cur, topic, pn);
				// KeywordsMentionedMostMapping
				new StackForumKeywordsMentionedMostMappingHourly().groupByKeyword(listByPN).save(hourly, cur, topic, pn);
				// VoCDetail
				new StackForumVoCDetailHourly().save(hourly, cur, topic, pn, listByPN);
			}
		}
	}

}
