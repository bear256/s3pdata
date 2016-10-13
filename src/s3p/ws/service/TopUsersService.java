package s3p.ws.service;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.topusers.MSForumTopUser;
import s3p.data.endpoint.topusers.StackForumTopUser;
import s3p.data.endpoint.topusers.TwitterTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class TopUsersService {

	private static final String ENDPOINT = Endpoint.TOPUSER;

	public String get(String platform, int topNum, String topic, String pnScope) {
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s:%09d", ENDPOINT, topic, pnScope, topNum - 1);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		String json = null;
		switch (platform) {
		case Platform.Twitter:
			json = twitter(docs);
			break;
		case Platform.MSDN:
		case Platform.TechNet:
			json = msforum(docs);
			break;
		case Platform.ServerFault:
		case Platform.StackOverflow:
		case Platform.SuperUser:
			json = stackforum(docs);
			break;
		}
		return json;
	}

	private String twitter(List<DocEntity> docs) {
		List<TwitterTopUser> topUsers = new ArrayList<>();
		for (DocEntity doc : docs) {
			topUsers.add(JSON.parseObject(doc.getJson(), TwitterTopUser.class));
		}
		return JSON.toJSONString(topUsers);
	}

	private String msforum(List<DocEntity> docs) {
		List<MSForumTopUser> topUsers = new ArrayList<>();
		for (DocEntity doc : docs) {
			topUsers.add(JSON.parseObject(doc.getJson(), MSForumTopUser.class));
		}
		return JSON.toJSONString(topUsers);
	}
	
	private String stackforum(List<DocEntity> docs) {
		List<StackForumTopUser> topUsers = new ArrayList<>();
		for (DocEntity doc : docs) {
			topUsers.add(JSON.parseObject(doc.getJson(), StackForumTopUser.class));
		}
		return JSON.toJSONString(topUsers);
	}

}
