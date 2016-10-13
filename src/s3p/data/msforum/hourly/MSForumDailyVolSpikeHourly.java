package s3p.data.msforum.hourly;

import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyvolspikes.DailyVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumDailyVolSpikeHourly {

	private static final String ENDPOINT = Endpoint.DAILYVOLSPIKE;

	private DailyVolSpike dailyVolSpike = new DailyVolSpike();

	public MSForumDailyVolSpikeHourly total(List<MSForum> listByPN) {
		for (MSForum msForum : listByPN) {
			int sentiment = msForum.getSentimentscore();
			int influenceCount = msForum.getViews();
			dailyVolSpike.incVocInfluence(sentiment, influenceCount);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic) {
		long timeslot = dt.getMilliseconds(TimeZone.getTimeZone("GMT+0")) / 1000;
		dailyVolSpike.setDailytimeslot(timeslot);
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), "ALL", dt.format("hh"));
		String json = JSON.toJSONString(dailyVolSpike);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
