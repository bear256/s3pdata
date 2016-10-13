package s3p.data.twitter.daily;

import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyinfluence.DailyInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterDailyInfluenceDaily {

	private static final String ENDPOINT = Endpoint.DAILYINFLUENCE;

	private DailyInfluence dailyInfluence = new DailyInfluence();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new TwitterDailyInfluenceDaily().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public TwitterDailyInfluenceDaily merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			DailyInfluence influence = JSON.parseObject(json, DailyInfluence.class);
			dailyInfluence.getVocinfluence().merge(influence.getVocinfluence());
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		dailyInfluence.setAttachedobject(dt.getMilliseconds(TimeZone.getTimeZone("GMT+0")) / 1000);
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s", ENDPOINT, topic.toUpperCase(), pn);
		String json = JSON.toJSONString(dailyInfluence);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
