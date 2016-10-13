package s3p.data.msforum.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.CreatedBy;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.attachedobject.MSForumAttachedObject;
import s3p.data.endpoint.topusers.MSForumTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumTopUserHourly {

	private static final String ENDPOINT = Endpoint.TOPUSER;

	private Map<String, MSForumTopUser> map = new HashMap<>();

	public MSForumTopUserHourly groupByUserId(List<MSForum> listByPN) {
		for (MSForum msForum : listByPN) {
			CreatedBy createdBy = msForum.getCreatedBy();
			String userId = createdBy.getUserId();
			int sentiment = msForum.getSentimentscore();
			int influenceCount = msForum.getViews();
			MSForumAttachedObject attachedObject = new MSForumAttachedObject(createdBy);
			MSForumTopUser topUser = new MSForumTopUser(attachedObject);
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
			MSForumTopUser topUser = map.get(userId);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"), userId);
			String json = JSON.toJSONString(topUser);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
