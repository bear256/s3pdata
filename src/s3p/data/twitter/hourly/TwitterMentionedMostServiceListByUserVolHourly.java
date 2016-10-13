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
import s3p.data.endpoint.mentionedmost.MentionedMostServiceByUserVol;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.ServiceNameUtils;
import s3p.data.utils.TableUtils;

public class TwitterMentionedMostServiceListByUserVolHourly {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELISTBYUSERVOL;

	private Map<String, MentionedMostServiceByUserVol> map = new HashMap<>();

	private Map<String, Set<String>> userIdServices = new HashMap<>();

	public TwitterMentionedMostServiceListByUserVolHourly groupByService(List<Twitter> listByPN) {
		for (Twitter twitter : listByPN) {
			User user = twitter.getUser();
			String userId = user.getId_str();
			String topic = twitter.getTopic();
			String text = twitter.getText();
			int sentiment = twitter.getSentimentscore();
			int influenceCount = user.getFollowers_count() + user.getFriends_count();
			List<String> servicenames = ServiceNameUtils.match4Twitter(topic, text);
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
