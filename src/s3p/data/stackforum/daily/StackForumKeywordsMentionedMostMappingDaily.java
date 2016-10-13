package s3p.data.stackforum.daily;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.KeywordMentionedMostMapping;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.AnomalyDetectionUtils;
import s3p.data.utils.TableUtils;
import s3p.data.utils.anomalydetection.RequestBody;

public class StackForumKeywordsMentionedMostMappingDaily {

	private static final String ENDPOINT = Endpoint.KEYWORDSMENTIONEDMOSTMAPPING;

	private Map<String, KeywordMentionedMostMapping> map = new HashMap<>();

	private Map<String, Set<String>> userIdKeywords = new HashMap<>();

	private Map<String, List<String[]>> keywordSpikes = new HashMap<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new StackForumKeywordsMentionedMostMappingDaily().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public StackForumKeywordsMentionedMostMappingDaily merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String[] fields = doc.getRowKey().split(":");
			String[] eTP = fields[0].split("-");
			String json = doc.getJson();
			if (eTP.length > 3) {
				String userId = fields[2];
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
					List<String[]> data = new ArrayList<>();
					keywordSpikes.put(keyword, data);
				} else {
					map.get(keyword).getVocinfluence().merge(keywordMentionedMostMapping.getVocinfluence());
				}
				List<String[]> data = keywordSpikes.get(keyword);
				String[] row = new String[2];
				DateTime cur = new DateTime(doc.getPartitionKey() + " " + fields[1] + ":00:00");
				row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
				row[1] = "" + keywordMentionedMostMapping.getVocinfluence().getVoctotalvol();
				data.add(row);
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String keyword : map.keySet()) {
			List<String[]> data = keywordSpikes.get(keyword);
			RequestBody requestBody = new RequestBody(data);
			List<String> spikeTimes = AnomalyDetectionUtils.listSpikeTime(requestBody);
			KeywordMentionedMostMapping keywordMentionedMostMapping = map.get(keyword);
			keywordMentionedMostMapping.getVocinfluence().setDetectedspikesvol(spikeTimes.size());
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, keyword);
			String json = JSON.toJSONString(keywordMentionedMostMapping);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
		for (String userId : userIdKeywords.keySet()) {
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, "USER", userId);
			String json = JSON.toJSONString(userIdKeywords.get(userId));
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
