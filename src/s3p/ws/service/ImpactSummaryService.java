package s3p.ws.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.impactsummary.ImpactSummary;
import s3p.data.endpoint.impactsummary.MostDislikedService;
import s3p.data.endpoint.impactsummary.MostLikedService;
import s3p.data.endpoint.impactsummary.MostMentionedService;
import s3p.data.endpoint.mentionedmost.MentionedMostService;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class ImpactSummaryService {

	private static final String ENDPOINT = Endpoint.IMPACTSUMMARY;

	public String get(String platform, String topic, int days) {
		String tableName = Platform.getWeeklyTableName(platform);
		DateTime now = DateTime.now(TimeZone.getTimeZone("GMT+0"));
		String partitionKey = now.minusDays(1).format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		String rowKey2 = String.format("%s-%s-%s", ENDPOINT, topic, "ALL");
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		System.out.println(partitionKey + "-" + rowKey1 + "-" + rowKey2 + ":" + docs.size());
		ImpactSummary impactSummary = null;
		Map<String, Object> map = new HashMap<>();
		if (!docs.isEmpty()) {
			impactSummary = JSON.parseObject(docs.get(0).getJson(), ImpactSummary.class);
			MostDislikedService[] dislikes = impactSummary.getMostdislikedservice();
			System.out.println(JSON.toJSONString(dislikes));
			for (MostDislikedService service : dislikes) {
				if (service == null)
					continue;
				String sn = service.getMentionedmostservice().getAttachedobject();
				List<DocEntity> lastDocs = TableUtils.filterDocs(tableName, now.minusDays(7).format("YYYY-MM-DD"),
						String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.NEG, sn),
						String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.NEG, sn));
				int voclasttimetotalvolume = 0;
				if (!lastDocs.isEmpty()) {
					voclasttimetotalvolume = JSON.parseObject(lastDocs.get(0).getJson(), MentionedMostService.class)
							.getVocinfluence().getVoctotalvol();
				}
				service.getMentionedmostservice().getVocinfluence().setVoclasttimetotalvolume(voclasttimetotalvolume);
			}
			MostLikedService[] likes = impactSummary.getMostlikedservice();
			for (MostLikedService service : likes) {
				if (service == null)
					continue;
				String sn = service.getMentionedmostservice().getAttachedobject();
				List<DocEntity> lastDocs = TableUtils.filterDocs(tableName, now.minusDays(7).format("YYYY-MM-DD"),
						String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.POSI, sn),
						String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.POSI, sn));
				int voclasttimetotalvolume = 0;
				if (!lastDocs.isEmpty()) {
					voclasttimetotalvolume = JSON.parseObject(lastDocs.get(0).getJson(), MentionedMostService.class)
							.getVocinfluence().getVoctotalvol();
				}
				service.getMentionedmostservice().getVocinfluence().setVoclasttimetotalvolume(voclasttimetotalvolume);
			}
			map.put("influenceofusers", impactSummary.getInfluenceofusers());
			map.put("joinedusers", impactSummary.getJoinedusers());
			map.put("mentionedservicecount", impactSummary.getMentionedservicecount());
			map.put("mostmentionedservice", genMostMentionedServices(platform, topic, now.minusDays(1)));
			map.put("rankingservice", genGrowthRanking(platform, topic, now.minusDays(1)));
			map.put("mostdislikedservice", impactSummary.getMostdislikedservice());
			map.put("mostlikedservice", impactSummary.getMostlikedservice());
			map.put("regionofusers", impactSummary.getRegionofusers());
			map.put("vocinsights", impactSummary.getVocinsights());
			map.put("mostnegfrom", impactSummary.getMostnegfrom());
			map.put("mostposifrom", impactSummary.getMostposifrom());
		} else{
			impactSummary = new ImpactSummary();
		}
		return JSON.toJSONString(map);
	}
	
	private MostMentionedService[] genMostMentionedServices(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL),
				String.format("%s-%s-%s:z", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL));
		System.out.println(JSON.toJSONString(docs.size()));
		List<MentionedMostService> alls = new ArrayList<>();
		int total = 0;
		for (DocEntity doc : docs) {
			MentionedMostService all = JSON.parseObject(doc.getJson(), MentionedMostService.class);
			total += all.getVocinfluence().getVoctotalvol();
			alls.add(all);
		}
		Collections.sort(alls, new Comparator<MentionedMostService>() {

			@Override
			public int compare(MentionedMostService o1, MentionedMostService o2) {
				// TODO Auto-generated method stub
				return o1.getVocinfluence().getVoctotalvol() < o2.getVocinfluence().getVoctotalvol() ? 1 : -1;
			}
		});
		MostMentionedService[] list = new MostMentionedService[3];
		for (int i = 0; i < alls.size() && i < 3; i++) {
			MentionedMostService all = alls.get(i);
			int occupyratio = all.getVocinfluence().getVoctotalvol() * 100 / (total != 0 ? total : 1);
			System.out.println(all.getVocinfluence().getVoctotalvol()+"\t"+total);
			list[i] = new MostMentionedService(all, occupyratio);
		}
		for (MostMentionedService service : list) {
			if (service == null)
				continue;
			String sn = service.getMentionedmostservice().getAttachedobject();
			List<DocEntity> lastDocs = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
					String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL, sn),
					String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL, sn));
			int voclasttimetotalvolume = 0;
			if (!lastDocs.isEmpty()) {
				voclasttimetotalvolume = JSON.parseObject(lastDocs.get(0).getJson(), MentionedMostService.class)
						.getVocinfluence().getVoctotalvol();
			}
			service.getMentionedmostservice().getVocinfluence().setVoclasttimetotalvolume(voclasttimetotalvolume);
		}
		return list;
	}
	
	private MostMentionedService[] genGrowthRanking(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL),
				String.format("%s-%s-%s:z", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL));
		System.out.println(JSON.toJSONString(docs.size()));
		List<MentionedMostService> alls = new ArrayList<>();
		int total = 0;
		for (DocEntity doc : docs) {
			MentionedMostService all = JSON.parseObject(doc.getJson(), MentionedMostService.class);
			total += all.getVocinfluence().getVoctotalvol();
			alls.add(all);
		}
		List<MostMentionedService> list = new ArrayList<>();
		for (MentionedMostService all: alls) {
			int occupyratio = all.getVocinfluence().getVoctotalvol() * 100 / (total != 0 ? total : 1);
			MostMentionedService service = new MostMentionedService(all, occupyratio);
			if (service == null)
				continue;
			String sn = service.getMentionedmostservice().getAttachedobject();
			List<DocEntity> lastDocs = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
					String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL, sn),
					String.format("%s-%s-%s:%s", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.ALL, sn));
			int voclasttimetotalvolume = 0;
			if (!lastDocs.isEmpty()) {
				voclasttimetotalvolume = JSON.parseObject(lastDocs.get(0).getJson(), MentionedMostService.class)
						.getVocinfluence().getVoctotalvol();
			}
			service.getMentionedmostservice().getVocinfluence().setVoclasttimetotalvolume(voclasttimetotalvolume);
			list.add(service);
		}
		Collections.sort(list, new Comparator<MostMentionedService>() {

			@Override
			public int compare(MostMentionedService o1, MostMentionedService o2) {
				// TODO Auto-generated method stub
				return o1.getMentionedmostservice().getVocinfluence().getVocvolgrowthratio() < o2.getMentionedmostservice().getVocinfluence().getVocvolgrowthratio() ? 1 : -1;
			}
		});	
		MostMentionedService[] services = new MostMentionedService[3];
		for (int i = 0; i < list.size() && i < 3; i++) {
			MostMentionedService service = list.get(i);
			services[i] = service;
		}
		return services;
	}

}
