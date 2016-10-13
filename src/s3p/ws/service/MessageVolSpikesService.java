package s3p.ws.service;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class MessageVolSpikesService {
	
	private static final String ENDPOINT = Endpoint.MESSAGEVOLSPIKE;

	public String get(String platform, String topic, String pnScope, int days) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		List<MessageVolSpike> list = new ArrayList<>();
		for (DocEntity doc : docs) {
			list.add(JSON.parseObject(doc.getJson(), MessageVolSpike.class));
		}
		return JSON.toJSONString(list);
	}

	public String getByDate(String platform, String topic, String pnScope, String date) {
		String tableName = Platform.getDailyTableName(platform);
		String partitionKey = new DateTime(date).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		List<MessageVolSpike> list = new ArrayList<>();
		for (DocEntity doc : docs) {
			list.add(JSON.parseObject(doc.getJson(), MessageVolSpike.class));
		}
		return JSON.toJSONString(list);
	}

}
