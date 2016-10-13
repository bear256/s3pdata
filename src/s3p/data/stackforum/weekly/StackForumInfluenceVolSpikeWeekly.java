package s3p.data.stackforum.weekly;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.InfluenceVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumInfluenceVolSpikeWeekly {

	private static final String ENDPOINT = Endpoint.INFLUENCEVOLSPIKE;
	private List<InfluenceVolSpike> list = new ArrayList<InfluenceVolSpike>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new StackForumInfluenceVolSpikeWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public StackForumInfluenceVolSpikeWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			InfluenceVolSpike influenceVolSpike = JSON.parseObject(json, InfluenceVolSpike.class);
			list.add(influenceVolSpike);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (InfluenceVolSpike influenceVolSpike : list) {
			long timeslot = influenceVolSpike.getAttachedobject().getTimeslot();
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(influenceVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
