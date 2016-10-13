package s3p.ws.service;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.documentdb.msforum.MSForum;
import s3p.data.documentdb.stackforum.StackForum;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.ServiceNameUtils;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class VoCDetailsService {

	private static final String ENDPOINT = Endpoint.VOCDETAIL;

	private List<Object> transfer(String platform, List<DocEntity> docs) {
		List<Object> list = new ArrayList<>();
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			switch (platform) {
			case Platform.Twitter:
				list.add(JSON.parseObject(json, Twitter.class));
				break;
			case Platform.MSDN:
			case Platform.TechNet:
				list.add(JSON.parseObject(json, MSForum.class));
				break;
			case Platform.ServerFault:
			case Platform.StackOverflow:
			case Platform.SuperUser:
				list.add(JSON.parseObject(json, StackForum.class));
				break;
			}
		}
		return list;
	}

	private List<Object> filterByUserId(String platform, List<DocEntity> docs, String userId) {
		List<Object> list = new ArrayList<>();
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			switch (platform) {
			case Platform.Twitter:
				Twitter twitter = JSON.parseObject(json, Twitter.class);
				if (userId.equals(twitter.userId())) {
					list.add(twitter);
				}
				break;
			case Platform.MSDN:
			case Platform.TechNet:
				MSForum msForum = JSON.parseObject(json, MSForum.class);
				if (userId.equals(msForum.userId())) {
					list.add(msForum);
				}
				break;
			case Platform.ServerFault:
			case Platform.StackOverflow:
			case Platform.SuperUser:
				StackForum stackForum = JSON.parseObject(json, StackForum.class);
				if (userId.equals(stackForum.userId())) {
					list.add(stackForum);
				}
				break;
			}
		}
		return list;
	}

	public String getByUser(String platform, String topic, String userId, String pnScope, int days) {
		String tableName = Platform.getHourlyTableName(platform);
		DateTime end = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1);
		DateTime start = end.minusDays(days - 1);
		List<Object> list = new ArrayList<>();
		for (DateTime cur = start; cur.lteq(end); cur = cur.plusDays(1)) {
			String partitionKey = cur.format("YYYY-MM-DD");
			String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
			String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
			List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
			list.addAll(filterByUserId(platform, docs, userId));
		}
		return JSON.toJSONString(list);
	}

	public String getByDate(String platform, String topic, long date, String pnScope, int days) {
		String tableName = Platform.getHourlyTableName(platform);
		DateTime dt = DateTime.forInstant(date * 1000, TimeZone.getTimeZone("GMT+0"));
		// DateTime dt = new DateTime("2016-09-03 22:00:00");
		String partitionKey = dt.format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:%s:0", ENDPOINT, topic, pnScope, dt.format("hh"));
		String rowKey2 = String.format("%s-%s-%s:%s:z", ENDPOINT, topic, pnScope, dt.format("hh"));
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		List<Object> list = transfer(platform, docs);
		return JSON.toJSONString(list);
	}

	public String getByPN(String platform, String topic, String pnScope, int days) {
		String tableName = Platform.getHourlyTableName(platform);
		DateTime end = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1);
		DateTime start = end.minusDays(days - 1);
		List<Object> list = new ArrayList<>();
		for (DateTime cur = start; cur.lteq(end); cur = cur.plusDays(1)) {
			String partitionKey = cur.format("YYYY-MM-DD");
			String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
			String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
			List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
			list.addAll(transfer(platform, docs));
		}
		return JSON.toJSONString(list);
	}

	public String getByServiceName(String platform, String topic, String serviceName, String pnScope, int days) {
		String tableName = Platform.getHourlyTableName(platform);
		DateTime end = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1);
		DateTime start = end.minusDays(days - 1);
		List<Object> list = new ArrayList<>();
		for (DateTime cur = start; cur.lteq(end); cur = cur.plusDays(1)) {
			String partitionKey = cur.format("YYYY-MM-DD");
			String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
			String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
			List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
			list.addAll(filterByServiceName(platform, docs, serviceName));
		}
		return JSON.toJSONString(list);
	}

	private List<Object> filterByServiceName(String platform, List<DocEntity> docs, String serviceName) {
		List<Object> list = new ArrayList<>();
		String text = "";
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			switch (platform) {
			case Platform.Twitter:
				Twitter twitter = JSON.parseObject(json, Twitter.class);
				text = twitter.getText();
				if (ServiceNameUtils.isMatch4Twitter(text, serviceName)) {
					list.add(twitter);
				}
				break;
			case Platform.MSDN:
			case Platform.TechNet:
				MSForum msForum = JSON.parseObject(json, MSForum.class);
				text = msForum.getForum().getDisplayName();
				if (ServiceNameUtils.isMatch4MSForum(text, serviceName)) {
					list.add(msForum);
				}
				break;
			case Platform.ServerFault:
			case Platform.StackOverflow:
			case Platform.SuperUser:
				StackForum stackForum = JSON.parseObject(json, StackForum.class);
				text = stackForum.getBody();
				if (ServiceNameUtils.isMatch4StackForum(text, serviceName)) {
					list.add(stackForum);
				}
				break;
			}
		}
		return list;
	}

}
