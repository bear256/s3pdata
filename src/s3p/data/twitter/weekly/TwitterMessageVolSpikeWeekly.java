package s3p.data.twitter.weekly;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterMessageVolSpikeWeekly implements Weekly {

	private static final String ENDPOINT = Endpoint.MESSAGEVOLSPIKE;
	private List<MessageVolSpike> list = new ArrayList<MessageVolSpike>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new TwitterMessageVolSpikeWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public TwitterMessageVolSpikeWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			MessageVolSpike messageVolSpike = JSON.parseObject(json, MessageVolSpike.class);
			list.add(messageVolSpike);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (MessageVolSpike messageVolSpike : list) {
			long timeslot = messageVolSpike.getAttachedobject().getTimeslot();
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(messageVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
