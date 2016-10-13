package s3p.ws.service;

import java.util.List;
import java.util.TimeZone;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class DailyVolSpikesService {
	
	private static final String ENDPOINT = Endpoint.DAILYVOLSPIKE;

	public String get(String platform, String topic, int days) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		String rowKey2 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		String json = "[]";
		if(!docs.isEmpty()) {
			json = docs.get(0).getJson();
		}
		return json;
	}

}
