package s3p.data.msforum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.config.S3PDataConfig;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.msforum.hourly.MSForumDailyInfluenceHourly;
import s3p.data.msforum.hourly.MSForumDailyVolSpikeHourly;
import s3p.data.msforum.hourly.MSForumInfluenceVolSpikeHourly;
import s3p.data.msforum.hourly.MSForumKeywordsMentionedMostMappingHourly;
import s3p.data.msforum.hourly.MSForumMentionedMostServiceListByUserVolHourly;
import s3p.data.msforum.hourly.MSForumMentionedMostServiceListHourly;
import s3p.data.msforum.hourly.MSForumMessageVolSpikeHourly;
import s3p.data.msforum.hourly.MSForumPNDistributionHourly;
import s3p.data.msforum.hourly.MSForumTopUserHourly;
import s3p.data.msforum.hourly.MSForumUserVolSpikeHourly;
import s3p.data.msforum.hourly.MSForumVoCDetailHourly;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumHourly {

	public static List<DocEntity> listDocs(String tableName, String partitionKey) {
		return TableUtils.readDocs(tableName, partitionKey);
	}

	public static Map<String, List<MSForum>> groupByTopic(List<DocEntity> docs) {
		Map<String, List<MSForum>> map = new HashMap<>();
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			MSForum msForum = JSON.parseObject(json, MSForum.class);
			String topic = msForum.topic();
			if (!map.containsKey(topic)) {
				List<MSForum> list = new ArrayList<>();
				map.put(topic, list);
			}
			List<MSForum> list = map.get(topic);
			list.add(msForum);
		}
		return map;
	}

	public static Map<String, List<MSForum>> groupByPN(List<MSForum> listByTopic) {
		Map<String, List<MSForum>> map = new HashMap<>();
		for (MSForum msForum : listByTopic) {
			int sentimentScore = msForum.getSentimentscore();
			String sentiment = Sentiment.get(sentimentScore);
			if (!map.containsKey(sentiment)) {
				List<MSForum> listByPN = new ArrayList<>();
				map.put(sentiment, listByPN);
			}
			List<MSForum> listByPN = map.get(sentiment);
			listByPN.add(msForum);
		}
		if (map.get(Sentiment.ALL) == null) {
			List<MSForum> listByPN = new ArrayList<>();
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
		Map<String, List<MSForum>> topics = groupByTopic(docs);
		for (String topic : topics.keySet()) {
			List<MSForum> listByTopic = topics.get(topic);
			// PNDistribution
			new MSForumPNDistributionHourly().totalVocinfluence(listByTopic).save(hourly, cur, topic);
			// DailyVolSpike
			new MSForumDailyVolSpikeHourly().total(listByTopic).save(hourly, cur, topic);
			Map<String, List<MSForum>> pns = groupByPN(listByTopic);
			for (String pn : pns.keySet()) {
				List<MSForum> listByPN = pns.get(pn);
				// TopUser
				new MSForumTopUserHourly().groupByUserId(listByPN).save(hourly, cur, topic, pn);
				// RegionDistribution
				// DailyInfluence
				new MSForumDailyInfluenceHourly().totalVocinfluence(listByPN).save(hourly, cur, topic, pn);
				// MessageVolSpike
				new MSForumMessageVolSpikeHourly().total(listByPN).save(hourly, cur, topic, pn);
				// InfluenceVolSpike
				new MSForumInfluenceVolSpikeHourly().total(listByPN).save(hourly, cur, topic, pn);
				// UserVolSpike
				new MSForumUserVolSpikeHourly().groupByUserId(listByPN).save(hourly, cur, topic, pn);
				// UserRegionVolSpike
				// MentionedMostServiceList
				new MSForumMentionedMostServiceListHourly().groupByService(listByPN).save(hourly, cur, topic, pn);
				// MentionedMostServiceListByUserVol
				new MSForumMentionedMostServiceListByUserVolHourly().groupByService(listByPN).save(hourly, cur, topic,
						pn);
				// KeywordsMentionedMostMapping
				new MSForumKeywordsMentionedMostMappingHourly().groupByKeyword(listByPN).save(hourly, cur, topic, pn);
				// VoCDetail
				new MSForumVoCDetailHourly().save(hourly, cur, topic, pn, listByPN);
			}
		}
	}
	
	public static void main(String[] args) {
		S3PDataConfig config = new S3PDataConfig();
		TableUtils.init(config);
		run("MSDNRaw", new DateTime("2016-10-26 06:00:00"), "MSDNHourly");
	}

}
