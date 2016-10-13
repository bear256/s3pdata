package s3p.data.stackforum.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.stackforum.Owner;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.attachedobject.StackForumAttachedObject;
import s3p.data.endpoint.topusers.StackForumTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumTopUserHourly {

	private static final String ENDPOINT = Endpoint.TOPUSER;

	private Map<String, StackForumTopUser> map = new HashMap<>();

	public StackForumTopUserHourly groupByUserId(List<StackForum> listByPN) {
		for (StackForum stackForum : listByPN) {
			Owner owner = stackForum.getOwner();
			if(owner == null) continue;
			String userId = "" + owner.getUser_id();
			int sentiment = stackForum.getSentimentscore();
			int influenceCount = stackForum.getView_count();
			StackForumAttachedObject attachedObject = new StackForumAttachedObject(owner);
			StackForumTopUser topUser = new StackForumTopUser(attachedObject);
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
			StackForumTopUser topUser = map.get(userId);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"), userId);
			String json = JSON.toJSONString(topUser);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
