package s3p.data.webjob;

import java.io.UnsupportedEncodingException;
import java.util.TimeZone;

import hirondelle.date4j.DateTime;
import hirondelle.date4j.DateTime.DayOverflow;
import s3p.data.config.S3PDataConfig;
import s3p.data.storage.table.JobEntity;
import s3p.data.twitter.TwitterDaily;
import s3p.data.twitter.TwitterHourly;
import s3p.data.twitter.TwitterWeekly;
import s3p.data.utils.AnomalyDetectionUtils;
import s3p.data.utils.TableUtils;

public class TwitterTotalJob {

	public static void run(S3PDataConfig config, String platform) {
		String raw = config.get("job." + platform + ".raw.table");
		String hourly = config.get("job." + platform + ".hourly.table");
		String daily = config.get("job." + platform + ".daily.table");
		String weekly = config.get("job." + platform + ".weekly.table");
		String jobName = config.get("job." + platform + ".total.job");
		JobEntity job = TableUtils.readJob(jobName);
		DateTime start = new DateTime(job.getStart());
		DateTime end = DateTime.now(TimeZone.getTimeZone("GMT+0"));
		DateTime cur = new DateTime(new DateTime(job.getLast()).format("YYYY-MM-DD 00:00:00"));
		while (cur.lteq(end)) {
			System.out.println(cur);
			// Hourly
			TwitterHourly.run(raw, cur, hourly);
			// Daily
			if ("23".equals(cur.format("hh"))) {
				TwitterDaily.run(platform, hourly, cur, daily);
				// Weekly
				if (start.numDaysFrom(cur) >= 6) {
					TwitterWeekly.run(platform, daily, cur, start, weekly);
				}
			}
			job.setLast(cur.format("YYYY-MM-DD hh:00:00"));
			TableUtils.writeJob(jobName, job);
			cur = cur.plus(0, 0, 0, 1, 0, 0, 0, DayOverflow.Abort);
		}
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		if (args.length == 2) {
			String filename = args[0];
			String platform = args[1];
			System.out.println(filename + "\t" + platform);
			S3PDataConfig config = new S3PDataConfig(filename);
			TableUtils.init(config);
			AnomalyDetectionUtils.init(config);
			run(config, platform);
		}
	}

}
