package s3p.data.twitter.daily;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.regiondistribution.RegionDistribution;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterRegionDistributionDaily {

	private static final String ENDPOINT = Endpoint.REGIONDISTRIBUTION;

	private Map<String, RegionDistribution> map = new HashMap<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new TwitterRegionDistributionDaily().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	private TwitterRegionDistributionDaily merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			RegionDistribution regionDistribution = JSON.parseObject(json, RegionDistribution.class);
			String region = regionDistribution.getAttachedobject();
			if (!map.containsKey(region)) {
				map.put(region, regionDistribution);
			} else {
				map.get(region).getVocinfluence().merge(regionDistribution.getVocinfluence());
			}
		}
		return this;
	}

	private void save(String tableName, DateTime dt, String topic, String pn) {
		for (String region : map.keySet()) {
			RegionDistribution regionDistribution = map.get(region);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, region);
			String json = JSON.toJSONString(regionDistribution);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
