package s3p.ws.service;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostService;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class MentionedMostServiceListService {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELIST;

	public String get(String platform, String topic, String pnScope, long date, String datetype) {
		String tableName = Platform.getWeeklyTableName(platform);
		DateTime dt = date == 0 ? DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1)
				: DateTime.forInstant(date * 1000, TimeZone.getTimeZone("GMT+0"));
		String partitionKey = dt.format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
		switch (datetype) {
		case "d":
			tableName = Platform.getDailyTableName(platform);
			rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
			rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
			break;
		case "h":
			tableName = Platform.getHourlyTableName(platform);
			rowKey1 = String.format("%s-%s-%s:%s:0", ENDPOINT, topic, pnScope, dt.format("hh"));
			rowKey2 = String.format("%s-%s-%s:%s:z", ENDPOINT, topic, pnScope, dt.format("hh"));
			break;
		default: // Weekly
			break;
		}
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		List<MentionedMostService> list = new ArrayList<>();
		for (DocEntity doc : docs) {
			list.add(JSON.parseObject(doc.getJson(), MentionedMostService.class));
		}
		return JSON.toJSONString(list);
	}

}
