package s3p.data.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.storage.table.DocEntity;
import s3p.data.twitter.hourly.TwitterDailyInfluenceHourly;
import s3p.data.twitter.hourly.TwitterDailyVolSpikeHourly;
import s3p.data.twitter.hourly.TwitterInfluenceVolSpikeHourly;
import s3p.data.twitter.hourly.TwitterKeywordsMentionedMostMappingHourly;
import s3p.data.twitter.hourly.TwitterMentionedMostServiceListByUserVolHourly;
import s3p.data.twitter.hourly.TwitterMentionedMostServiceListHourly;
import s3p.data.twitter.hourly.TwitterMessageVolSpikeHourly;
import s3p.data.twitter.hourly.TwitterPNDistributionHourly;
import s3p.data.twitter.hourly.TwitterRegionDistributionHourly;
import s3p.data.twitter.hourly.TwitterTopUserHourly;
import s3p.data.twitter.hourly.TwitterUserRegionVolSpikeHourly;
import s3p.data.twitter.hourly.TwitterUserVolSpikeHourly;
import s3p.data.twitter.hourly.TwitterVoCDetailHourly;
import s3p.data.utils.TableUtils;

public class TwitterHourly {

	public static List<DocEntity> listDocs(String tableName, String partitionKey) {
		return TableUtils.readDocs(tableName, partitionKey);
	}

	public static Map<String, List<Twitter>> groupByTopic(List<DocEntity> docs) {
		Map<String, List<Twitter>> map = new HashMap<>();
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			Twitter twitter = JSON.parseObject(json, Twitter.class);
			String topic = twitter.getTopic();
			if (!map.containsKey(topic)) {
				List<Twitter> list = new ArrayList<>();
				map.put(topic, list);
			}
			List<Twitter> list = map.get(topic);
			list.add(twitter);
		}
		return map;
	}

	public static Map<String, List<Twitter>> groupByPN(List<Twitter> listByTopic) {
		Map<String, List<Twitter>> map = new HashMap<>();
		for (Twitter twitter : listByTopic) {
			int sentimentScore = twitter.getSentimentscore();
			String sentiment = Sentiment.get(sentimentScore);
			if (!map.containsKey(sentiment)) {
				List<Twitter> listByPN = new ArrayList<>();
				map.put(sentiment, listByPN);
			}
			List<Twitter> listByPN = map.get(sentiment);
			listByPN.add(twitter);
		}
		if (map.get(Sentiment.ALL) == null) {
			List<Twitter> listByPN = new ArrayList<>();
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
		Map<String, List<Twitter>> topics = groupByTopic(docs);
		for (String topic : topics.keySet()) {
			List<Twitter> listByTopic = topics.get(topic);
			// PNDistribution
			new TwitterPNDistributionHourly().totalVocinfluence(listByTopic).save(hourly, cur, topic);
			// DailyVolSpike
			new TwitterDailyVolSpikeHourly().total(listByTopic).save(hourly, cur, topic);
			Map<String, List<Twitter>> pns = groupByPN(listByTopic);
			for (String pn : pns.keySet()) {
				List<Twitter> listByPN = pns.get(pn);
				// TopUser
				new TwitterTopUserHourly().groupByUserId(listByPN).save(hourly, cur, topic, pn);
				// RegionDistribution
				new TwitterRegionDistributionHourly().groupByRegion(listByPN).save(hourly, cur, topic, pn);
				// DailyInfluence
				new TwitterDailyInfluenceHourly().totalVocinfluence(listByPN).save(hourly, cur, topic, pn);
				// MessageVolSpike
				new TwitterMessageVolSpikeHourly().total(listByPN).save(hourly, cur, topic, pn);
				// InfluenceVolSpike
				new TwitterInfluenceVolSpikeHourly().total(listByPN).save(hourly, cur, topic, pn);
				// UserVolSpike
				new TwitterUserVolSpikeHourly().groupByUserId(listByPN).save(hourly, cur, topic, pn);
				// UserRegionVolSpike
				new TwitterUserRegionVolSpikeHourly().groupByRegion(listByPN).save(hourly, cur, topic, pn);
				// MentionedMostServiceList
				new TwitterMentionedMostServiceListHourly().groupByService(listByPN).save(hourly, cur, topic, pn);
				// MentionedMostServiceListByUserVol
				new TwitterMentionedMostServiceListByUserVolHourly().groupByService(listByPN).save(hourly, cur, topic,
						pn);
				// KeywordsMentionedMostMapping
				new TwitterKeywordsMentionedMostMappingHourly().groupByKeyword(listByPN).save(hourly, cur, topic, pn);
				// VoCDetail
				new TwitterVoCDetailHourly().save(hourly, cur, topic, pn, listByPN);
			}
		}
	}

}
