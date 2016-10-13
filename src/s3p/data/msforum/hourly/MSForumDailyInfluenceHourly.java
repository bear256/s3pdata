package s3p.data.msforum.hourly;

import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyinfluence.DailyInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumDailyInfluenceHourly {

	private static final String ENDPOINT = Endpoint.DAILYINFLUENCE;

	private DailyInfluence dailyInfluence = new DailyInfluence();

	public MSForumDailyInfluenceHourly totalVocinfluence(List<MSForum> listByPN) {
		for (MSForum msForum : listByPN) {
			int sentiment = msForum.getSentimentscore();
			int influenceCount = msForum.getViews();
			dailyInfluence.incVocInfluence(sentiment, influenceCount);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"));
		String json = JSON.toJSONString(dailyInfluence);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
