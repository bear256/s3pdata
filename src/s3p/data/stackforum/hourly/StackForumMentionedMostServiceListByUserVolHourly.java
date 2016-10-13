package s3p.data.stackforum.hourly;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.stackforum.Owner;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostServiceByUserVol;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.ServiceNameUtils;
import s3p.data.utils.TableUtils;

public class StackForumMentionedMostServiceListByUserVolHourly {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELISTBYUSERVOL;

	private Map<String, MentionedMostServiceByUserVol> map = new HashMap<>();

	private Map<String, Set<String>> userIdServices = new HashMap<>();

	public StackForumMentionedMostServiceListByUserVolHourly groupByService(String topic, List<StackForum> listByPN) {
		for (StackForum stackForum : listByPN) {
			Owner owner = stackForum.getOwner();
			if (owner == null)
				continue;
			String userId = "" + owner.getUser_id();
			int sentiment = stackForum.getSentimentscore();
			int influenceCount = stackForum.getView_count();
			String text = stackForum.getBody();
			List<String> servicenames = ServiceNameUtils.match4StackForum(topic, text);
			for (String service : servicenames) {
				MentionedMostServiceByUserVol mentionedMostServiceByUserVol = new MentionedMostServiceByUserVol(
						service);
				mentionedMostServiceByUserVol.incVocInfluence(sentiment, influenceCount);
				if (!map.containsKey(service)) {
					map.put(service, mentionedMostServiceByUserVol);
				} else {
					map.get(service).getVocinfluence().merge(mentionedMostServiceByUserVol.getVocinfluence());
				}
			}
			if (!userIdServices.containsKey(userId)) {
				Set<String> sns = new HashSet<>();
				userIdServices.put(userId, sns);
			}
			Set<String> sns = userIdServices.get(userId);
			sns.addAll(servicenames);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String service : map.keySet()) {
			MentionedMostServiceByUserVol mentionedMostServiceByUserVol = map.get(service);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"),
					service);
			String json = JSON.toJSONString(mentionedMostServiceByUserVol);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
		for (String userId : userIdServices.keySet()) {
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, "USER",
					dt.format("hh"), userId);
			String json = JSON.toJSONString(userIdServices.get(userId));
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
