package s3p.data.stackforum.weekly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.topusers.StackForumTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;

public class StackForumTopUserWeekly {

	private static final String ENDPOINT = Endpoint.TOPUSER;

	private List<StackForumTopUser> list = new ArrayList<>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic,
			String pn) {
		if (ENDPOINT.equals(endpoint)) {
			new StackForumTopUserWeekly().merge(listByPN).save(tableName, dt, topic, pn);
		}
	}

	public StackForumTopUserWeekly merge(List<DocEntity> listByPN) {
		Map<String, StackForumTopUser> map = new HashMap<>();
		for (DocEntity doc : listByPN) {
			String json = doc.getJson();
			StackForumTopUser topUser = JSON.parseObject(json, StackForumTopUser.class);
			String userId = "" + topUser.getAttachedobject().getUser_id();
			if (!map.containsKey(userId)) {
				map.put(userId, topUser);
				list.add(topUser);
			} else {
				map.get(userId).getVocinfluence().merge(topUser.getVocinfluence());
			}
		}
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		Collections.sort(list, new Comparator<StackForumTopUser>() {

			@Override
			public int compare(StackForumTopUser o1, StackForumTopUser o2) {
				int order = 0;
				switch (pn) {
				case Sentiment.UNDEF:
					order = o1.getVocinfluence().getUndefinedtotalvol() < o2.getVocinfluence().getUndefinedtotalvol() ? 1
							: -1;
					break;
				case Sentiment.NEG:
					order = o1.getVocinfluence().getNegativetotalvol() < o2.getVocinfluence().getNegativetotalvol() ? 1
							: -1;
					break;
				case Sentiment.NEU:
					order = o1.getVocinfluence().getNeutraltotalvol() < o2.getVocinfluence().getNeutraltotalvol() ? 1
							: -1;
					break;
				case Sentiment.POSI:
					order = o1.getVocinfluence().getPositivetotalvol() < o2.getVocinfluence().getPositivetotalvol() ? 1
							: -1;
					break;
				default:
					order = o1.getVocinfluence().getVoctotalvol() < o2.getVocinfluence().getVoctotalvol() ? 1 : -1;
					break;
				}
				return order;
			}
		});
		for (int index = 0; index < list.size(); index++) {
			StackForumTopUser topUser = list.get(index);
			String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
			String rowKey = String.format("%s-%s-%s:%09d", ENDPOINT, topic.toUpperCase(), pn, index);
			String json = JSON.toJSONString(topUser);
			DocEntity entity = new DocEntity(partitionKey, rowKey, json);
			TableUtils.writeEntity(tableName, entity);
		}
	}
}
