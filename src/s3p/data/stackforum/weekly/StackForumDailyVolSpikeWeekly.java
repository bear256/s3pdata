package s3p.data.stackforum.weekly;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyvolspikes.DailyVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumDailyVolSpikeWeekly {

	private static final String ENDPOINT = Endpoint.DAILYVOLSPIKE;

	private List<DailyVolSpike> list = new ArrayList<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic) {
		if (ENDPOINT.equals(endpoint)) {
			new StackForumDailyVolSpikeWeekly().merge(listByPN).save(tableName, dt, topic, "ALL");
		}
	}

	public StackForumDailyVolSpikeWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			DailyVolSpike dailyVolSpike = JSON.parseObject(json, DailyVolSpike.class);
			list.add(dailyVolSpike);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s", ENDPOINT, topic.toUpperCase(), "ALL");
		String json = JSON.toJSONString(list);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
