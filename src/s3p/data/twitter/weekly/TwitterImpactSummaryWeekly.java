package s3p.data.twitter.weekly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.common.VocInfluence;
import s3p.data.endpoint.impactsummary.ImpactSummary;
import s3p.data.endpoint.impactsummary.InfluenceOfUsers;
import s3p.data.endpoint.impactsummary.JoinedUsers;
import s3p.data.endpoint.impactsummary.MentionedServiceCount;
import s3p.data.endpoint.impactsummary.MostDislikedService;
import s3p.data.endpoint.impactsummary.MostLikedService;
import s3p.data.endpoint.impactsummary.RegionOfUsers;
import s3p.data.endpoint.impactsummary.VocInsights;
import s3p.data.endpoint.mentionedmost.MentionedMostService;
import s3p.data.endpoint.regiondistribution.RegionDistribution;
import s3p.data.endpoint.volspikes.InfluenceVolSpike;
import s3p.data.endpoint.volspikes.MessageVolSpike;
import s3p.data.endpoint.volspikes.UserRegionVolSpike;
import s3p.data.endpoint.volspikes.UserVolSpike;
import s3p.data.storage.table.DocEntity;
import s3p.data.utils.TableUtils;
import s3p.ws.config.Platform;

public class TwitterImpactSummaryWeekly {

	private static final String ENDPOINT = Endpoint.IMPACTSUMMARY;

	private ImpactSummary impactSummary;

	public TwitterImpactSummaryWeekly gen(String platform, String topic, DateTime dt) {
		InfluenceOfUsers influenceofusers = genInfluenceOfUsers(platform, topic, dt);
		JoinedUsers joinedusers = genJoinedUsers(platform, topic, dt);
		MentionedServiceCount mentionedservicecount = genMentionedServiceCount(platform, topic, dt);
		MostDislikedService[] mostdislikedservice = genMostDislikedServices(platform, topic, dt);
		MostLikedService[] mostlikedservice = genMostLikedServices(platform, topic, dt);
		RegionDistribution mostnegfrom = genMostPNFrom(platform, topic, Sentiment.NEG, dt);
		RegionDistribution mostposifrom = genMostPNFrom(platform, topic, Sentiment.POSI, dt);
		RegionOfUsers regionofusers = genRegionOfUsers(platform, topic, dt);
		VocInsights vocinsights = genVocInsights(platform, topic, dt);
		impactSummary = new ImpactSummary(influenceofusers, joinedusers, mentionedservicecount, mostdislikedservice,
				mostlikedservice, mostnegfrom, mostposifrom, regionofusers, vocinsights);
		return this;
	}

	public void save(String tableName, DateTime dt, String topic, String pn) {
		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
		String rowKey = String.format("%s-%s-%s", ENDPOINT, topic.toUpperCase(), "ALL");
		String json = JSON.toJSONString(impactSummary);
		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
		TableUtils.writeEntity(tableName, entity);
	}

	private InfluenceOfUsers genInfluenceOfUsers(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		// objectcountthistime & detectedhourlyspikesvol
		List<DocEntity> influenceVolSpikes = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.INFLUENCEVOLSPIKE, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.INFLUENCEVOLSPIKE, topic, "ALL"));
		int objectcountthistime = 0;
		int detectedhourlyspikesvol = 0;
		for (DocEntity doc : influenceVolSpikes) {
			InfluenceVolSpike influenceVolSpike = JSON.parseObject(doc.getJson(), InfluenceVolSpike.class);
			objectcountthistime += influenceVolSpike.getVocinfluence().getVocinfluencedvol();
			if (influenceVolSpike.getAttachedobject().isIsspike())
				detectedhourlyspikesvol++;
		}
		detectedhourlyspikesvol = detectedhourlyspikesvol / (7 * 24);
		// objectcountlasttime
		List<DocEntity> lastInfluenceVolSpikes = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.INFLUENCEVOLSPIKE, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.INFLUENCEVOLSPIKE, topic, "ALL"));
		int objectcountlasttime = 0;
		for (DocEntity doc : lastInfluenceVolSpikes) {
			InfluenceVolSpike influenceVolSpike = JSON.parseObject(doc.getJson(), InfluenceVolSpike.class);
			objectcountlasttime += influenceVolSpike.getVocinfluence().getVocinfluencedvol();
		}
		// comparedratio
		double comparedratio = 1.0 * (objectcountthistime - objectcountlasttime)
				/ (objectcountlasttime != 0 ? objectcountlasttime : 1);
		InfluenceOfUsers influenceOfUsers = new InfluenceOfUsers(comparedratio, detectedhourlyspikesvol,
				objectcountlasttime, objectcountthistime);
		return influenceOfUsers;
	}

	private JoinedUsers genJoinedUsers(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		// objectcountthistime
		List<DocEntity> topUsers = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.TOPUSER, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.TOPUSER, topic, "ALL"));
		int objectcountthistime = topUsers.size();
		// objectcountlasttime & comparedratio
		List<DocEntity> lastTopUsers = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.TOPUSER, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.TOPUSER, topic, "ALL"));
		int objectcountlasttime = lastTopUsers.size();
		double comparedratio = 1.0 * (objectcountthistime - objectcountlasttime)
				/ (objectcountlasttime != 0 ? objectcountlasttime : 1);
		// detectedhourlyspikesvol
		List<DocEntity> userVolSpikes = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.USERVOLSPIKE, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.USERVOLSPIKE, topic, "ALL"));
		int detectedhourlyspikesvol = 0;
		for (DocEntity doc : userVolSpikes) {
			UserVolSpike userVolSpike = JSON.parseObject(doc.getJson(), UserVolSpike.class);
			if (userVolSpike.getAttachedobject().isIsspike())
				detectedhourlyspikesvol++;
		}
		detectedhourlyspikesvol = detectedhourlyspikesvol / (7 * 24);
		JoinedUsers joinedusers = new JoinedUsers(comparedratio, detectedhourlyspikesvol, objectcountlasttime,
				objectcountthistime);
		return joinedusers;
	}

	private MentionedServiceCount genMentionedServiceCount(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		// objectcountthistime
		List<DocEntity> mentionedServices = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MENTIONEDMOSTSERVICELIST, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.MENTIONEDMOSTSERVICELIST, topic, "ALL"));
		int objectcountthistime = mentionedServices.size();
		// objectcountlasttime & comparedratio
		List<DocEntity> lastMentionedServices = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MENTIONEDMOSTSERVICELIST, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.MENTIONEDMOSTSERVICELIST, topic, "ALL"));
		int objectcountlasttime = lastMentionedServices.size();
		double comparedratio = 1.0 * (objectcountthistime - objectcountlasttime)
				/ (objectcountlasttime != 0 ? objectcountlasttime : 1);
		// detectedhourlyspikesvol
		int detectedhourlyspikesvol = 0;
		MentionedServiceCount mentionedservicecount = new MentionedServiceCount(comparedratio, detectedhourlyspikesvol,
				objectcountlasttime, objectcountthistime);
		return mentionedservicecount;
	}

	private MostDislikedService[] genMostDislikedServices(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.NEG),
				String.format("%s-%s-%s:z", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.NEG));
		List<MentionedMostService> negs = new ArrayList<>();
		int total = 0;
		for (DocEntity doc : docs) {
			MentionedMostService neg = JSON.parseObject(doc.getJson(), MentionedMostService.class);
			total += neg.getVocinfluence().getNegativetotalvol();
			negs.add(neg);
		}
		Collections.sort(negs, new Comparator<MentionedMostService>() {

			@Override
			public int compare(MentionedMostService o1, MentionedMostService o2) {
				// TODO Auto-generated method stub
				return o1.getVocinfluence().getNegativetotalvol() < o2.getVocinfluence().getNegativetotalvol() ? 1 : -1;
			}
		});
		MostDislikedService[] list = new MostDislikedService[3];
		for (int i = 0; i < negs.size() && i < 3; i++) {
			MentionedMostService neg = negs.get(i);
			int occupyratio = neg.getVocinfluence().getNegativetotalvol() * 100 / (total != 0 ? total : 1);
			list[i] = new MostDislikedService(neg, occupyratio);
		}
		return list;
	}

	private MostLikedService[] genMostLikedServices(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.POSI),
				String.format("%s-%s-%s:z", Endpoint.MENTIONEDMOSTSERVICELIST, topic, Sentiment.POSI));
		List<MentionedMostService> posis = new ArrayList<>();
		int total = 0;
		for (DocEntity doc : docs) {
			MentionedMostService posi = JSON.parseObject(doc.getJson(), MentionedMostService.class);
			total += posi.getVocinfluence().getNegativetotalvol();
			posis.add(posi);
		}
		Collections.sort(posis, new Comparator<MentionedMostService>() {

			@Override
			public int compare(MentionedMostService o1, MentionedMostService o2) {
				// TODO Auto-generated method stub
				return o1.getVocinfluence().getPositivetotalvol() < o2.getVocinfluence().getPositivetotalvol() ? 1 : -1;
			}
		});
		MostLikedService[] list = new MostLikedService[3];
		for (int i = 0; i < posis.size() && i < 3; i++) {
			MentionedMostService posi = posis.get(i);
			int occupyratio = posi.getVocinfluence().getNegativetotalvol() * 100 / (total != 0 ? total : 1);
			list[i] = new MostLikedService(posi, occupyratio);
		}
		return list;
	}

	private RegionDistribution genMostPNFrom(String platform, String topic, String pnScope, DateTime dt) {
		String endpoint = Endpoint.REGIONDISTRIBUTION;
		String tableName = Platform.getWeeklyTableName(platform);
		String partitionKey = dt.format("YYYY-MM-DD");
		String rowKey1 = String.format("%s-%s-%s:0", endpoint, topic, pnScope);
		String rowKey2 = String.format("%s-%s-%s:z", endpoint, topic, pnScope);
		List<DocEntity> docs = TableUtils.filterDocs(tableName, partitionKey, rowKey1, rowKey2);
		List<RegionDistribution> list = new ArrayList<>();
		for (DocEntity doc : docs) {
			list.add(JSON.parseObject(doc.getJson(), RegionDistribution.class));
		}
		Collections.sort(list, new Comparator<RegionDistribution>() {

			@Override
			public int compare(RegionDistribution o1, RegionDistribution o2) {
				int order = 0;
				switch (pnScope) {
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
		RegionDistribution mostPNFrom = list.isEmpty() ? new RegionDistribution() : list.get(0);
		return mostPNFrom;
	}

	private RegionOfUsers genRegionOfUsers(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		// objectcountthistime
		List<DocEntity> regions = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.REGIONDISTRIBUTION, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.REGIONDISTRIBUTION, topic, "ALL"));
		int objectcountthistime = regions.size();
		// objectcountlasttime
		List<DocEntity> lastRegions = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.REGIONDISTRIBUTION, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.REGIONDISTRIBUTION, topic, "ALL"));
		int objectcountlasttime = lastRegions.size();
		// comparedratio
		double comparedratio = 1.0 * (objectcountthistime - objectcountlasttime)
				/ (objectcountlasttime != 0 ? objectcountlasttime : 1);
		// detectedhourlyspikesvol
		List<DocEntity> regionVolSpikes = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.USERREGIONVOLSPIKE, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.USERREGIONVOLSPIKE, topic, "ALL"));
		int detectedhourlyspikesvol = 0;
		for (DocEntity doc : regionVolSpikes) {
			UserRegionVolSpike regionVolSpike = JSON.parseObject(doc.getJson(), UserRegionVolSpike.class);
			if (regionVolSpike.getAttachedobject().isIsspike())
				detectedhourlyspikesvol++;
		}
		detectedhourlyspikesvol = detectedhourlyspikesvol / (7 * 24);
		RegionOfUsers regionOfUsers = new RegionOfUsers(comparedratio, detectedhourlyspikesvol, objectcountlasttime,
				objectcountthistime);
		return regionOfUsers;
	}

	private VocInsights genVocInsights(String platform, String topic, DateTime dt) {
		String tableName = Platform.getWeeklyTableName(platform);
		List<DocEntity> msgVolSpikes = TableUtils.filterDocs(tableName, dt.format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MESSAGEVOLSPIKE, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.MESSAGEVOLSPIKE, topic, "ALL"));
		VocInfluence objectcountthistime = new VocInfluence();
		for (DocEntity doc : msgVolSpikes) {
			MessageVolSpike messageVolSpike = JSON.parseObject(doc.getJson(), MessageVolSpike.class);
			objectcountthistime.merge(messageVolSpike.getVocinfluence());
		}
		List<DocEntity> lastMsgVolSpikes = TableUtils.filterDocs(tableName, dt.minusDays(7).format("YYYY-MM-DD"),
				String.format("%s-%s-%s:0", Endpoint.MESSAGEVOLSPIKE, topic, "ALL"),
				String.format("%s-%s-%s:z", Endpoint.MESSAGEVOLSPIKE, topic, "ALL"));
		VocInfluence objectcountlasttime = new VocInfluence();
		for (DocEntity doc : lastMsgVolSpikes) {
			MessageVolSpike messageVolSpike = JSON.parseObject(doc.getJson(), MessageVolSpike.class);
			objectcountlasttime.merge(messageVolSpike.getVocinfluence());
		}
		double comparedratio = 1.0 * (objectcountthistime.getVoctotalvol() - objectcountlasttime.getVoctotalvol())
				/ objectcountlasttime.getVoctotalvol();
		int detectedhourlyspikesvol = objectcountthistime.getDetectedspikesvol() / (7 * 24);
		VocInsights vocInsights = new VocInsights(comparedratio, detectedhourlyspikesvol, objectcountlasttime,
				objectcountthistime);
		return vocInsights;
	}
}
