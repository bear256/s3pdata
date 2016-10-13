package s3p.data.msforum.weekly;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyinfluence.DailyInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumDailyInfluenceWeekly {

	private static final String ENDPOINT = Endpoint.DAILYINFLUENCE;

	private List<DailyInfluence> list = new ArrayList<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumDailyInfluenceWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public MSForumDailyInfluenceWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			DailyInfluence dailyInfluence = JSON.parseObject(json, DailyInfluence.class);
			list.add(dailyInfluence);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s", ENDPOINT, topic.toUpperCase(), pn);
		String json = JSON.toJSONString(list);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
