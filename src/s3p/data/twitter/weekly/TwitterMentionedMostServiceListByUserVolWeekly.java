package s3p.data.twitter.weekly;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostServiceByUserVol;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterMentionedMostServiceListByUserVolWeekly implements Weekly {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELISTBYUSERVOL;

	private Map<String, MentionedMostServiceByUserVol> map = new HashMap<>();

	private Map<String, Set<String>> userIdServices = new HashMap<>();

	private Map<String, Integer> serviceUsers = new HashMap<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new TwitterMentionedMostServiceListByUserVolWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public TwitterMentionedMostServiceListByUserVolWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String[] fields = doc.getRowKey().split(":");
			String[] eTP = fields[0].split("-");
			String json = doc.getJson();
			if (eTP.length > 3) {
				String userId = fields[1];
				@SuppressWarnings("unchecked")
				Set<String> sns = JSON.parseObject(json, HashSet.class);
				if (!userIdServices.containsKey(userId)) {
					userIdServices.put(userId, sns);
				} else {
					userIdServices.get(userId).addAll(sns);
				}
			} else {
				MentionedMostServiceByUserVol mentionedMostServiceByUserVol = JSON.parseObject(json,
						MentionedMostServiceByUserVol.class);
				String service = mentionedMostServiceByUserVol.getAttachedobject();
				if (!map.containsKey(service)) {
					map.put(service, mentionedMostServiceByUserVol);
				} else {
					map.get(service).getVocinfluence().merge(mentionedMostServiceByUserVol.getVocinfluence());
				}
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String userId : userIdServices.keySet()) {
			for (String service : userIdServices.get(userId)) {
				if (!serviceUsers.containsKey(service)) {
					serviceUsers.put(service, 0);
				}
				Integer userCount = serviceUsers.get(service) + 1;
				serviceUsers.put(service, userCount);
			}
		}
		for (String service : map.keySet()) {
			MentionedMostServiceByUserVol mentionedMostServiceByUserVol = map.get(service);
			mentionedMostServiceByUserVol.getVocinfluence().setUniqueusers(serviceUsers.get(service));
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, service);
			String json = JSON.toJSONString(mentionedMostServiceByUserVol);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
