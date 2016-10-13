package s3p.data.msforum.weekly;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.KeywordMentionedMostMapping;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumKeywordsMentionedMostMappingWeekly {

	private static final String ENDPOINT = Endpoint.KEYWORDSMENTIONEDMOSTMAPPING;

	private Map<String, KeywordMentionedMostMapping> map = new HashMap<>();

	private Map<String, Set<String>> userIdKeywords = new HashMap<>();

	private Map<String, Integer> keywordUsers = new HashMap<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumKeywordsMentionedMostMappingWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public MSForumKeywordsMentionedMostMappingWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String[] fields = doc.getRowKey().split(":");
			String[] eTP = fields[0].split("-");
			String json = doc.getJson();
			if (eTP.length > 3) {
				String userId = fields[1];
				@SuppressWarnings("unchecked")
				Set<String> sns = JSON.parseObject(json, HashSet.class);
				if (!userIdKeywords.containsKey(userId)) {
					userIdKeywords.put(userId, sns);
				} else {
					userIdKeywords.get(userId).addAll(sns);
				}
			} else {
				KeywordMentionedMostMapping keywordMentionedMostMapping = JSON.parseObject(json,
						KeywordMentionedMostMapping.class);
				String keyword = keywordMentionedMostMapping.getAttachedobject();
				if (!map.containsKey(keyword)) {
					map.put(keyword, keywordMentionedMostMapping);
				} else {
					map.get(keyword).getVocinfluence().merge(keywordMentionedMostMapping.getVocinfluence());
				}
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String userId : userIdKeywords.keySet()) {
			for (String keyword : userIdKeywords.get(userId)) {
				if (!keywordUsers.containsKey(keyword)) {
					keywordUsers.put(keyword, 0);
				}
				Integer userCount = keywordUsers.get(keyword) + 1;
				keywordUsers.put(keyword, userCount);
			}
		}
		for (String keyword : map.keySet()) {
			KeywordMentionedMostMapping keywordMentionedMostMapping = map.get(keyword);
			keywordMentionedMostMapping.getVocinfluence().setUniqueusers(keywordUsers.get(keyword));
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, keyword);
			String json = JSON.toJSONString(keywordMentionedMostMapping);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
