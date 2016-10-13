package s3p.data.msforum.hourly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostService;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.ServiceNameUtils;
import s3p.data.utils.TableUtils;

public class MSForumMentionedMostServiceListHourly {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELIST;

	private Map<String, MentionedMostService> map = new HashMap<>();

	public MSForumMentionedMostServiceListHourly groupByService(List<MSForum> listByPN) {
		for (MSForum msForum : listByPN) {
			String topic = msForum.topic();
			int sentiment = msForum.getSentimentscore();
			int influenceCount = msForum.getViews();
			String text = msForum.getForum().getDisplayName();
			List<String> services = ServiceNameUtils.match4MSForum(topic, text);
			for (String service : services) {
				MentionedMostService mentionedMostService = new MentionedMostService(service);
				mentionedMostService.incVocInfluence(sentiment, influenceCount);
				if (!map.containsKey(service)) {
					map.put(service, mentionedMostService);
				} else {
					map.get(service).getVocinfluence().merge(mentionedMostService.getVocinfluence());
				}
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String service : map.keySet()) {
			MentionedMostService mentionedMostService = map.get(service);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s:%s", ENDPOINT, topic.toUpperCase(), pn, dt.format("hh"),
					service);
			String json = JSON.toJSONString(mentionedMostService);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
