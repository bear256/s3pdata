package s3p.data.twitter.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import hirondelle.date4j.DateTime;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.documentdb.twitter.User;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.regiondistribution.RegionDistribution;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.data.utils.URLCodeUtils;

public class TwitterRegionDistributionHourly {

	private static final String ENDPOINT = Endpoint.REGIONDISTRIBUTION;

	private Map<String, RegionDistribution> map = new HashMap<>();

	public TwitterRegionDistributionHourly groupByRegion(List<Twitter> listByPN) {
		for (Twitter twitter : listByPN) {
			User user = twitter.getUser();
			String region = URLCodeUtils.encode(user.getLocation());
			int sentiment = twitter.getSentimentscore();
			int influenceCount = user.getFriends_count() + user.getFollowers_count();
			RegionDistribution regionDistribution = new RegionDistribution(region);
			regionDistribution.incVocInfluence(sentiment, influenceCount);
			if (!map.containsKey(region)) {
				map.put(region, regionDistribution);
			} else {
				map.get(region).getVocinfluence().merge(regionDistribution.getVocinfluence());
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String region : map.keySet()) {
			RegionDistribution regionDistribution = map.get(region);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"), region);
			String json = JSON.toJSONString(regionDistribution);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
