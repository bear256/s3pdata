package s3p.data.stackforum.hourly;

import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.VocInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumPNDistributionHourly {

	private static final String ENDPOINT = Endpoint.PNDISTRIBUTION;

	private VocInfluence vocInfluence = new VocInfluence();

	public StackForumPNDistributionHourly totalVocinfluence(List<StackForum> listByPN) {
		for (StackForum stackForum : listByPN) {
			int sentiment = stackForum.getSentimentscore();
			int influenceCount = stackForum.getView_count();
			vocInfluence.incVocInfluence(sentiment, influenceCount);
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic) {
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), "ALL", dt.format("hh"));
		String json = JSON.toJSONString(vocInfluence);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}
}
