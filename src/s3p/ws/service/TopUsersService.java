package s3p.ws.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.topusers.MSForumTopUser;
import s3p.data.endpoint.topusers.StackForumTopUser;
import s3p.data.endpoint.topusers.TwitterTopUser;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class TopUsersService {

	private static final String ENDPOINT = Endpoint.TOPUSER;

	public String get(String platform, int topNum, String topic, String pnScope, long date, String datetype) {
		String tableName = Platform.getWeeklyTableName(platform);
		DateTime dt = date == 0 ? DateTime.now(TimeZone.getTimeZone("GMT+0")).minusDays(1)
				: DateTime.forInstant(date * 1000, TimeZone.getTimeZone("GMT+0"));
		String partitionKey = dt.format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s:%09d", ENDPOINT, topic, pnScope, topNum - 1);
		switch (datetype) {
		case "d":
			tableName = Platform.getDailyTableName(platform);
			rowKey1 = String.format("%s-%s-%s:0", ENDPOINT, topic, pnScope);
			rowKey2 = String.format("%s-%s-%s:z", ENDPOINT, topic, pnScope);
			break;
		case "h":
			tableName = Platform.getHourlyTableName(platform);
			rowKey1 = String.format("%s-%s-%s:%s:0", ENDPOINT, topic, pnScope, dt.format("hh"));
			rowKey2 = String.format("%s-%s-%s:%s:z", ENDPOINT, topic, pnScope, dt.format("hh"));
			break;
		default: // Weekly
			break;
		}
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		String json = null;
		switch (platform) {
		case Platform.Twitter:
			json = twitter(docs, pnScope, topNum);
			break;
		case Platform.MSDN:
		case Platform.TechNet:
			json = msforum(docs, pnScope, topNum);
			break;
		case Platform.ServerFault:
		case Platform.StackOverflow:
		case Platform.SuperUser:
			json = stackforum(docs, pnScope, topNum);
			break;
		}
		return json;
	}

	private String twitter(List<DocEntity> docs, String pn, int topNum) {
		List<TwitterTopUser> topUsers = new ArrayList<>();
		for (DocEntity doc : docs) {
			topUsers.add(JSON.parseObject(doc.getJson(), TwitterTopUser.class));
		}
		Collections.sort(topUsers, new Comparator<TwitterTopUser>() {

			@Override
			public int compare(TwitterTopUser o1, TwitterTopUser o2) {
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
		return JSON.toJSONString(take(topUsers, topNum));
	}

	private String msforum(List<DocEntity> docs, String pn, int topNum) {
		List<MSForumTopUser> topUsers = new ArrayList<>();
		for (DocEntity doc : docs) {
			topUsers.add(JSON.parseObject(doc.getJson(), MSForumTopUser.class));
		}
		Collections.sort(topUsers, new Comparator<MSForumTopUser>() {

			@Override
			public int compare(MSForumTopUser o1, MSForumTopUser o2) {
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
		return JSON.toJSONString(take(topUsers, topNum));
	}
	
	private String stackforum(List<DocEntity> docs, String pn, int topNum) {
		List<StackForumTopUser> topUsers = new ArrayList<>();
		for (DocEntity doc : docs) {
			topUsers.add(JSON.parseObject(doc.getJson(), StackForumTopUser.class));
		}
		Collections.sort(topUsers, new Comparator<StackForumTopUser>() {

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
		return JSON.toJSONString(take(topUsers, topNum));
	}
	
	private <T> List<T> take(List<T> list, int topNum) {
		List<T> objs = new ArrayList<>();
		int num = 0;
		for(T obj: list) {
			if(num < topNum) {
				objs.add(obj);
				num++;
			}
		}
		return objs;
	}

}
