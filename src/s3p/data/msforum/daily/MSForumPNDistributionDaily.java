package s3p.data.msforum.daily;

import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.VocInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumPNDistributionDaily {

	private static final String ENDPOINT = Endpoint.PNDISTRIBUTION;

	private VocInfluence vocInfluence = new VocInfluence();

	public static void run(String endpoint, List<DocEntity> listByTopic, String tableName, DateTime dt, String topic) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumPNDistributionDaily().merge(listByTopic).save(tableName, dt, topic);
		}
	}

	public MSForumPNDistributionDaily merge(List<DocEntity> listByTopic) {
		for (DocEntity doc : listByTopic) {
			String json = doc.getJson();
			VocInfluence influence = JSON.parseObject(json, VocInfluence.class);
			vocInfluence.merge(influence);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic) {
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s", ENDPOINT, topic.toUpperCase(), "ALL");
		String json = JSON.toJSONString(vocInfluence);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
