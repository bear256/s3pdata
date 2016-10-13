package s3p.data.stackforum.hourly;

import java.util.List;
import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumVoCDetailHourly {

	private static final String ENDPOINT = Endpoint.VOCDETAIL;
	
	public void save(String tableName, DateTime dt, String topic, String pn, List<StackForum> listByPN) {
		for (StackForum stackForum : listByPN) {
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"), stackForum.getId());
			String json = JSON.toJSONString(stackForum);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
