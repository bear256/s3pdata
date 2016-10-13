package s3p.data.msforum.weekly;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.UserVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumUserVolSpikeWeekly {

	private static final String ENDPOINT = Endpoint.USERVOLSPIKE;
	private List<UserVolSpike> list = new ArrayList<UserVolSpike>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumUserVolSpikeWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public MSForumUserVolSpikeWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			UserVolSpike userVolSpike = JSON.parseObject(json, UserVolSpike.class);
			list.add(userVolSpike);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (UserVolSpike userVolSpike : list) {
			long timeslot = userVolSpike.getAttachedobject().getTimeslot();
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(userVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
