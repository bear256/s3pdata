package s3p.data.stackforum.hourly;

import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumMessageVolSpikeHourly {

	private static final String ENDPOINT = Endpoint.MESSAGEVOLSPIKE;

	private MessageVolSpike messageVolSpike = new MessageVolSpike();

	public StackForumMessageVolSpikeHourly total(List<StackForum> listByPN) {
		for (StackForum stackForum : listByPN) {
			int sentiment = stackForum.getSentimentscore();
			int influenceCount = stackForum.getView_count();
			messageVolSpike.incVocInfluence(sentiment, influenceCount);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		long timeslot = dt.getMilliseconds(TimeZone.getTimeZone("GMT+0")) / 1000;
		messageVolSpike.getAttachedobject().setTimeslot(timeslot);
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
		String json = JSON.toJSONString(messageVolSpike);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
