package s3p.data.twitter.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.documentdb.twitter.User;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.attachedobject.TwitterAttachedObject;
import s3p.data.endpoint.topusers.TwitterTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterTopUserHourly {

	private static final String ENDPOINT = Endpoint.TOPUSER;

	private Map<String, TwitterTopUser> map = new HashMap<>();

	public TwitterTopUserHourly groupByUserId(List<Twitter> listByPN) {
		for (Twitter twitter : listByPN) {
			User user = twitter.getUser();
			String userId = user.getId_str();
			int sentiment = twitter.getSentimentscore();
			int influenceCount = user.getFriends_count() + user.getFollowers_count();
			TwitterAttachedObject attachedObject = new TwitterAttachedObject(user);
			TwitterTopUser topUser = new TwitterTopUser(attachedObject);
			topUser.incVocInfluence(sentiment, influenceCount);
			if (!map.containsKey(userId)) {
				map.put(userId, topUser);
			} else {
				map.get(userId).getVocinfluence().merge(topUser.getVocinfluence());
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String userId : map.keySet()) {
			TwitterTopUser topUser = map.get(userId);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"), userId);
			String json = JSON.toJSONString(topUser);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
