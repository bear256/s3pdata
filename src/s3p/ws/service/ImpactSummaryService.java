package s3p.ws.service;

import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
import s3p.data.endpoint.common.Sentiment;
import s3p.data.endpoint.impactsummary.ImpactSummary;
import s3p.data.endpoint.impactsummary.MostDislikedService;
import s3p.data.endpoint.impactsummary.MostLikedService;
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
		ImpactSummary impactSummary = JSON.parseObject(docs.get(0).getJson(), ImpactSummary.class);
		MostDislikedService[] dislikes = impactSummary.getMostdislikedservice();
		System.out.println(JSON.toJSONString(dislikes));
		for (MostDislikedService service : dislikes) {
			if(service == null) continue;
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
			if(service == null) continue;
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
		return JSON.toJSONString(impactSummary);
	}

}
