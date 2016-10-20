package s3p.ws.service;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.VocInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class PNDistributionService {

	private static final String ENDPOINT = Endpoint.PNDISTRIBUTION;

	public String get(String platform, String topic) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		String rowKey2 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		List<VocInfluence> pndistributions = new ArrayList<>();
		if (docs != null && !docs.isEmpty()) {
			for (DocEntity doc : docs) {
				pndistributions.add(JSON.parseObject(doc.getJson(), VocInfluence.class));
			}
		} else {
			pndistributions.add(new VocInfluence());
		}
		return JSON.toJSONString(pndistributions.get(0));
	}

}
