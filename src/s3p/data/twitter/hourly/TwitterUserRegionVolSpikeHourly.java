package s3p.data.twitter.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.documentdb.twitter.User;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.UserRegionVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterUserRegionVolSpikeHourly {

	private static final String ENDPOINT = Endpoint.USERREGIONVOLSPIKE;

	private UserRegionVolSpike userRegionVolSpike = new UserRegionVolSpike();

	public TwitterUserRegionVolSpikeHourly groupByRegion(List<Twitter> listByPN) {
		Map<String, String> map = new HashMap<>();
		for (Twitter twitter : listByPN) {
			User user = twitter.getUser();
			String region = user.getLocation();
			if (!map.containsKey(region)) {
				map.put(region, region);
			}
		}
		userRegionVolSpike.getVocinfluence().setUniqueuserregion(map.size());
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		long timeslot = dt.getMilliseconds(TimeZone.getTimeZone("GMT+0")) / 1000;
		userRegionVolSpike.getAttachedobject().setTimeslot(timeslot);
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
		String json = JSON.toJSONString(userRegionVolSpike);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
