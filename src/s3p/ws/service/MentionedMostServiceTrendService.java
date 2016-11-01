package s3p.ws.service;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import hirondelle.date4j.DateTime.DayOverflow;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostServiceTrend;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class MentionedMostServiceTrendService {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELIST;

	public String get(String platform, String topic, String pnScope, String serviceName) {
		String tableName = Platform.getHourlyTableName(platform);
		DateTime now = new DateTime(DateTime.now(TimeZone.getTimeZone("GMT+0")).format("YYYY-MM-DD 00:00:00"));
		DateTime start = now.minusDays(7);
		DateTime end = now.minusDays(1);
		List<MentionedMostServiceTrend> list = new ArrayList<>();
		for (DateTime cur = start; cur.lteq(end); cur = cur.plus(0, 0, 0, 1, 0, 0, 0, DayOverflow.Abort)) {
			String partitionKey = cur.format("YYYY-MM-DD");
			String rowKey1 = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic, pnScope, cur.format("hh"),
					"".equals(serviceName) ? "0" : serviceName);
			String rowKey2 = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic, pnScope, cur.format("hh"),
					"".equals(serviceName) ? "z" : serviceName);
			List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
			for (DocEntity doc : docs) {
				MentionedMostServiceTrend trend = JSON.parseObject(doc.getJson(), MentionedMostServiceTrend.class);
				long timeslot = new DateTime(doc.getPartitionKey() + " " + doc.getRowKey().split(":")[1] + ":00:00")
						.getMilliseconds(TimeZone.getTimeZone("GMT+0")) / 1000;
				trend.setTimeslot(timeslot);
				list.add(trend);
			}
		}
		return JSON.toJSONString(list);
	}
}
