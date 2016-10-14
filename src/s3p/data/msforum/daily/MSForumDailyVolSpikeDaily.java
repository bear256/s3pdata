package s3p.data.msforum.daily;

//import java.util.ArrayList;
import java.util.List;
//import java.util.Locale;
//import java.util.TimeZone;

//import com.alibaba.fastjson.JSON;

import hirondelle.date4j.DateTime;
import s3p.data.endpoint.common.Endpoint;
//import s3p.data.endpoint.dailyvolspikes.DailyVolSpike;
import s3p.data.storage.table.DocEntity;
//import s3p.data.utils.AnomalyDetectionUtils;
//import s3p.data.utils.TableUtils;
//import s3p.data.utils.anomalydetection.RequestBody;

public class MSForumDailyVolSpikeDaily {

	private static final String ENDPOINT = Endpoint.DAILYVOLSPIKE;

//	private DailyVolSpike dailyVolSpike = new DailyVolSpike();

//	private List<DailyVolSpike> list = new ArrayList<DailyVolSpike>();

	public static void run(String endpoint, List<DocEntity> listByPN, String tableName, DateTime dt, String topic) {
		if (ENDPOINT.equals(endpoint)) {
			new MSForumDailyVolSpikeDaily().detectMerge(listByPN).save(tableName, dt, topic);
		}
	}

	public MSForumDailyVolSpikeDaily detectMerge(List<DocEntity> listByPN) {
//		List<String[]> neg = new ArrayList<>();
//		List<String[]> neginfluence = new ArrayList<>();
//		List<String[]> posi = new ArrayList<>();
//		List<String[]> posiinfluence = new ArrayList<>();
//		List<String[]> total = new ArrayList<>();
//		List<String[]> totalinfluence = new ArrayList<>();
//		List<String> times = new ArrayList<>();
//		for (DocEntity doc : listByPN) {
//			String json = doc.getJson();
//			DailyVolSpike volSpike = JSON.parseObject(json, DailyVolSpike.class);
//			dailyVolSpike.merge(volSpike);
//			list.add(volSpike);
//			long timeslot = volSpike.getDailytimeslot();
//			DateTime cur = DateTime.forInstant(timeslot * 1000, TimeZone.getTimeZone("GMT+0"));
//			String time = cur.format("M/D/YYYY h12:00:00 a", Locale.US);
//			neg.add(new String[] { time, "" + volSpike.getDailynegvol() });
//			neginfluence.add(new String[] { time, "" + volSpike.getDailyneginfluencevol() });
//			posi.add(new String[] { time, "" + volSpike.getDailyposivol() });
//			posiinfluence.add(new String[] { time, "" + volSpike.getDailyposiinfluencevol() });
//			total.add(new String[] { time, "" + volSpike.getDailytotalvol() });
//			totalinfluence.add(new String[] { time, "" + volSpike.getDailytotalinfluencevol() });
//			times.add(time);
//		}
//		// Neg
//		RequestBody negRequestBody = new RequestBody(neg);
//		List<String> negSpikeTimes = AnomalyDetectionUtils.listSpikeTime(negRequestBody);
//		dailyVolSpike.setDailynegspike(negSpikeTimes.size());
//		RequestBody neginfluenceRequestBody = new RequestBody(neginfluence);
//		List<String> neginfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(neginfluenceRequestBody);
//		dailyVolSpike.setDailynegspike(neginfluenceSpikeTimes.size());
//		// Posi
//		RequestBody posiRequestBody = new RequestBody(posi);
//		List<String> posiSpikeTimes = AnomalyDetectionUtils.listSpikeTime(posiRequestBody);
//		dailyVolSpike.setDailynegspike(posiSpikeTimes.size());
//		RequestBody posiinfluenceRequestBody = new RequestBody(posiinfluence);
//		List<String> posiinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(posiinfluenceRequestBody);
//		dailyVolSpike.setDailynegspike(posiinfluenceSpikeTimes.size());
//		// Total
//		RequestBody totalRequestBody = new RequestBody(total);
//		List<String> totalSpikeTimes = AnomalyDetectionUtils.listSpikeTime(totalRequestBody);
//		dailyVolSpike.setDailynegspike(totalSpikeTimes.size());
//		RequestBody totalinfluenceRequestBody = new RequestBody(totalinfluence);
//		List<String> totalinfluenceSpikeTimes = AnomalyDetectionUtils.listSpikeTime(totalinfluenceRequestBody);
//		dailyVolSpike.setDailynegspike(totalinfluenceSpikeTimes.size());
		return this;
	}

	public void save(String tableName, DateTime dt, String topic) {
//		long dailytimeslot = new DateTime(dt.format("YYYY-MM-DD")).getMilliseconds(TimeZone.getTimeZone("GMT+0"))
//				/ 1000;
//		dailyVolSpike.setDailytimeslot(dailytimeslot);
//		String partitionKey = String.format("%s", dt.format("YYYY-MM-DD"));
//		String rowKey = String.format("%s-%s-%s", ENDPOINT, topic.toUpperCase(), "ALL");
//		String json = JSON.toJSONString(dailyVolSpike);
//		DocEntity entity = new DocEntity(partitionKey, rowKey, json);
//		TableUtils.writeEntity(tableName, entity);
	}
}
