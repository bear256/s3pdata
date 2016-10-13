package s3p.data.stackforum.daily;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.topusers.StackForumTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumTopUserDaily {
	private static final String ENDPOINT = Endpoint.TOPUSER;

	private Map<String, StackForumTopUser> map = new HashMap<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new StackForumTopUserDaily().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public StackForumTopUserDaily merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			StackForumTopUser topUser = JSON.parseObject(json, StackForumTopUser.class);
			String userId = "" + topUser.getAttachedobject().getUser_id();
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
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, userId);
			String json = JSON.toJSONString(topUser);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
