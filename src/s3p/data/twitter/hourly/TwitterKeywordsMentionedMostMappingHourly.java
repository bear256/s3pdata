package s3p.data.twitter.hourly;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.documentdb.twitter.User;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.mentionedmost.KeywordMentionedMostMapping;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.KeywordsUtils;
import s3p.data.utils.TableUtils;

public class TwitterKeywordsMentionedMostMappingHourly {

	private static final String ENDPOINT = Endpoint.KEYWORDSMENTIONEDMOSTMAPPING;

	private Map<String, KeywordMentionedMostMapping> map = new HashMap<>();

	private Map<String, Set<String>> userIdKeywords = new HashMap<>();

	public TwitterKeywordsMentionedMostMappingHourly groupByKeyword(List<Twitter> listByPN) {
		for (Twitter twitter : listByPN) {
			User user = twitter.getUser();
			String userId = user.getId_str();
			String text = twitter.getText();
			int sentiment = twitter.getSentimentscore();
			int influenceCount = user.getFriends_count() + user.getFollowers_count();
			List<String> keywords = null;
			switch (Sentiment.get(sentiment)) {
			case Sentiment.NEG:
				keywords = KeywordsUtils.match(Sentiment.NEG, text);
				break;
			case Sentiment.POSI:
				keywords = KeywordsUtils.match(Sentiment.POSI, text);
				break;
			default:
				return this;
			}
			for (String keyword : keywords) {
				KeywordMentionedMostMapping keywordMentionedMostMapping = new KeywordMentionedMostMapping(keyword);
				keywordMentionedMostMapping.incVocInfluence(sentiment, influenceCount);
				if (!map.containsKey(keyword)) {
					map.put(keyword, keywordMentionedMostMapping);
				} else {
					map.get(keyword).getVocinfluence().merge(keywordMentionedMostMapping.getVocinfluence());
				}
			}
			if (!userIdKeywords.containsKey(userId)) {
				Set<String> sns = new HashSet<>();
				userIdKeywords.put(userId, sns);
			}
			Set<String> sns = userIdKeywords.get(userId);
			sns.addAll(keywords);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String keyword : map.keySet()) {
			KeywordMentionedMostMapping keywordMentionedMostMapping = map.get(keyword);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"),
					keyword);
			String json = JSON.toJSONString(keywordMentionedMostMapping);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
		for (String userId : userIdKeywords.keySet()) {
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, "USER",
					dt.format("hh"), userId);
			String json = JSON.toJSONString(userIdKeywords.get(userId));
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
