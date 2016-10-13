package s3p.data.msforum.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.CreatedBy;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.UserVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumUserVolSpikeHourly {

	private static final String ENDPOINT = Endpoint.USERVOLSPIKE;

	private UserVolSpike userVolSpike = new UserVolSpike();

	public MSForumUserVolSpikeHourly groupByUserId(List<MSForum> listByPN) {
		Map<String, String> map = new HashMap<>();
		for (MSForum msForum : listByPN) {
			CreatedBy createdBy = msForum.getCreatedBy();
			String userId = createdBy.getUserId();
			if (!map.containsKey(userId)) {
				map.put(userId, userId);
			}
		}
		userVolSpike.getVocinfluence().setUniqueusers(map.size());
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		long timeslot = dt.getMilliseconds(TimeZone.getTimeZone("GMT+0")) / 1000;
		userVolSpike.getAttachedobject().setTimeslot(timeslot);
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
		String json = JSON.toJSONString(userVolSpike);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
