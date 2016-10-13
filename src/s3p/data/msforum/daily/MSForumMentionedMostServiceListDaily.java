package s3p.data.msforum.daily;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostService;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class MSForumMentionedMostServiceListDaily {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELIST;

	private Map<String, MentionedMostService> map = new HashMap<>();
	
	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumMentionedMostServiceListDaily().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}
	
	public MSForumMentionedMostServiceListDaily merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			MentionedMostService mentionedMostService = JSON.parseObject(json, MentionedMostService.class);
			String service = mentionedMostService.getAttachedobject();
			if(!map.containsKey(service)) {
				map.put(service, mentionedMostService);
			} else{
				map.get(service).getVocinfluence().merge(mentionedMostService.getVocinfluence());
			}
		}
		return this;
	}
	
	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String service : map.keySet()) {
			MentionedMostService mentionedMostService = map.get(service);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, service);
			String json = JSON.toJSONString(mentionedMostService);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
