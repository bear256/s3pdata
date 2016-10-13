package s3p.ws.service;

import java.io.IOException;
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
import s3p.data.utils.KeywordsUtils;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class SubPageVoCDetailsService {

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

	private List<Object> filterByKeywords(String platform, List<DocEntity> docs, String keywords,
			boolean isFuzzyQuery) {
		List<Object> list = new ArrayList<>();
		List<String> dict = null;
		if (isFuzzyQuery) {
			try {
				dict = KeywordsUtils.getAnotherWords(keywords);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			dict = new ArrayList<>();
			dict.add(keywords);
		}
		String text = "";
		for (DocEntity doc : docs) {
			String json = doc.getJson();
			for (String keyword : dict) {
				switch (platform) {
				case Platform.Twitter:
					Twitter twitter = JSON.parseObject(json, Twitter.class);
					text = twitter.getText();
					if (KeywordsUtils.isMatch(text, keyword)) {
						list.add(twitter);
					}
					break;
				case Platform.MSDN:
				case Platform.TechNet:
					MSForum msForum = JSON.parseObject(json, MSForum.class);
					text = msForum.getForum().getDisplayName();
					if (KeywordsUtils.isMatch(text, keyword)) {
						list.add(msForum);
					}
					break;
				case Platform.ServerFault:
				case Platform.StackOverflow:
				case Platform.SuperUser:
					StackForum stackForum = JSON.parseObject(json, StackForum.class);
					text = stackForum.getBody();
					if (KeywordsUtils.isMatch(text, keyword)) {
						list.add(stackForum);
					}
					break;
				}
			}
		}
		return list;
	}

	public String get4SubPage(String platform, String topic, Long date, String pnScope) {
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

	public String get4SubPageByKeywords(String platform, String topic, String keywords, String pnScope,
			boolean isFuzzyQuery, int days) {
		String tableName = Platform.getHourlyTableName(platform);
		DateTime end = DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1);
		DateTime start = end.minusDays(days - 1);
		List<Object> list = new ArrayList<>();
		for (DateTime cur = start; cur.lteq(end); cur = cur.plusDays(1)) {
			String partitionKey = cur.format("YYYY-MM-DD");
			String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
			String rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
			List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
			list.addAll(filterByKeywords(platform, docs, keywords, isFuzzyQuery));
		}
		return JSON.toJSONString(list);
	}

}
