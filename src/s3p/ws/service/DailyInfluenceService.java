package s3p.ws.service;

import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.dailyinfluence.DailyInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class DailyInfluenceService {
	
	private static final String ENDPOINT = Endpoint.DAILYINFLUENCE;

	public String get(String platform, String topic, String pnScope, int days) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s", ENDPOINT, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s", ENDPOINT, topic, pnScope);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		System.out.println(partitionKey+"-"+rowKey1+"-"+rowKey2+":"+docs.size());
		DailyInfluence[] dailyInfluences = null;
		if(docs != null && !docs.isEmpty()) {
			dailyInfluences = JSON.parseObject(docs.get(0).getJson(), DailyInfluence[].class);
		} else {
			dailyInfluences = new DailyInfluence[]{};
		}
		return JSON.toJSONString(dailyInfluences);
	}

}
