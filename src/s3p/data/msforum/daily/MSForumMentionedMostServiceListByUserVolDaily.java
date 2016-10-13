package s3p.data.msforum.daily;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.mentionedmost.MentionedMostServiceByUserVol;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.AnomalyDetectionUtils;
import s3p.data.utils.TableUtils;
import s3p.data.utils.anomalydetection.RequestBody;

public class MSForumMentionedMostServiceListByUserVolDaily {

	private static final String ENDPOINT = Endpoint.MENTIONEDMOSTSERVICELISTBYUSERVOL;

	private Map<String, MentionedMostServiceByUserVol> map = new HashMap<>();

	private Map<String, Set<String>> userIdServices = new HashMap<>();

	private Map<String, List<String[]>> serviceSpikes = new HashMap<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumMentionedMostServiceListByUserVolDaily().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public MSForumMentionedMostServiceListByUserVolDaily merge(List<DocEntity> listByPN) {
		for (DocEntity doc : listByPN) {
			String[] fields = doc.getRowKey().split(":");
			String[] eTP = fields[0].split("-");
			String json = doc.getJson();
			if (eTP.length > 3) {
				String userId = fields[2];
				@SuppressWarnings("unchecked")
				Set<String> sns = JSON.parseObject(json, HashSet.class);
				if (!userIdServices.containsKey(userId)) {
					userIdServices.put(userId, sns);
				} else {
					userIdServices.get(userId).addAll(sns);
				}
			} else {
				MentionedMostServiceByUserVol mentionedMostServiceByUserVol = JSON.parseObject(json,
						MentionedMostServiceByUserVol.class);
				String service = mentionedMostServiceByUserVol.getAttachedobject();
				if (!map.containsKey(service)) {
					map.put(service, mentionedMostServiceByUserVol);
					List<String[]> data = new ArrayList<>();
					serviceSpikes.put(service, data);
				} else {
					map.get(service).getVocinfluence().merge(mentionedMostServiceByUserVol.getVocinfluence());
				}
				List<String[]> data = serviceSpikes.get(service);
				String[] row = new String[2];
				DateTime cur = new DateTime(doc.getPartitionKey() + " " + fields[1] + ":00:00");
				row[0] = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
				row[1] = "" + mentionedMostServiceByUserVol.getVocinfluence().getVoctotalvol();
				data.add(row);
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		for (String service : map.keySet()) {
			List<String[]> data = serviceSpikes.get(service);
			RequestBody requestBody = new RequestBody(data);
			List<String> spikeTimes = AnomalyDetectionUtils.listSpikeTime(requestBody);
			MentionedMostServiceByUserVol mentionedMostServiceByUserVol = map.get(service);
			mentionedMostServiceByUserVol.getVocinfluence().setDetectedspikesvol(spikeTimes.size());
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, service);
			String json = JSON.toJSONString(mentionedMostServiceByUserVol);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
		for (String userId : userIdServices.keySet()) {
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s-%s:%s", ENDPOINT, topic.toUpperCase(), pn, "USER", userId);
			String json = JSON.toJSONString(userIdServices.get(userId));
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
