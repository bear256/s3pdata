package s3p.data.twitter.weekly;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.volspikes.UserRegionVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class TwitterUserRegionVolSpikeWeekly implements Weekly {

	private static final String ENDPOINT = Endpoint.USERREGIONVOLSPIKE;
	private List<UserRegionVolSpike> list = new ArrayList<UserRegionVolSpike>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new TwitterUserRegionVolSpikeWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public TwitterUserRegionVolSpikeWeekly merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			UserRegionVolSpike userRegionVolSpike = JSON.parseObject(json, UserRegionVolSpike.class);
			list.add(userRegionVolSpike);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (UserRegionVolSpike userRegionVolSpike : list) {
			long timeslot = userRegionVolSpike.getAttachedobject().getTimeslot();
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, timeslot);
			String json = JSON.toJSONString(userRegionVolSpike);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
