package s3p.ws.utils;

import java.util.TimeZone;

import hirondelle.date4j.DateTime;
import s3p.data.config.S3PDataConfig;

public class ConfigWrapper {
	
	private static S3PDataConfig config;
	
	public static void init(S3PDataConfig config0) {
		config = config0;
	}
	
	public static String get(String key) {
		return config.get(key);
	}
	
	public static void main(String[] args) {
		System.out.println(DateTime.forInstant(1471230900L*1000, TimeZone.getTimeZone("GMT+0")));
		System.out.println(DateTime.forInstant(1475596800L*1000, TimeZone.getTimeZone("GMT+0")));
		System.out.println(new DateTime("2016-01-01").getMilliseconds(TimeZone.getTimeZone("GMT+0")));
//		DateTime now = DateTime.now(TimeZone.getTimeZone("GMT+0"));
//		System.out.println("NOW: "+now);
//		DateTime start = now.minusDays(30);
//		DateTime end = now;
//		for (DateTime cur = start; cur.lteq(end); cur = cur.plusDays(1)) {
//			System.out.println("CUR: "+cur);
//		}
		System.out.println(new DateTime("2016-01-01").getSecond());
	}

}
