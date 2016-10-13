package s3p.data.msforum.hourly;

import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.VocInfluence;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumPNDistributionHourly {

	private static final String ENDPOINT = Endpoint.PNDISTRIBUTION;

	private VocInfluence vocInfluence = new VocInfluence();

	public MSForumPNDistributionHourly totalVocinfluence(List<MSForum> listByPN) {
		for (MSForum msForum : listByPN) {
			int sentiment = msForum.getSentimentscore();
			int influenceCount = msForum.getViews();
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
