package s3p.data.webjob;

import java.util.List;
import java.util.TimeZone;

import com.alibaba.fastjson.JSON;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentCollection;

import hirondelle.date4j.DateTime;
import hirondelle.date4j.DateTime.DayOverflow;
import s3p.data.config.S3PDataConfig;
import s3p.data.documentdb.msforum.MSDN;
import s3p.data.documentdb.msforum.TechNet;
import s3p.data.documentdb.stackforum.ServerFault;
import s3p.data.documentdb.stackforum.StackOverflow;
import s3p.data.documentdb.stackforum.SuperUser;
import s3p.data.documentdb.twitter.Twitter;
import s3p.data.storage.table.DocEntity;
import s3p.data.storage.table.JobEntity;
import s3p.data.utils.DocumentDbUtils;
import s3p.data.utils.TableUtils;

public class RawJob {

	public static <T> void run(S3PDataConfig config, String platform, Class<T> clazz) {
		String dbId = config.get("documentdb.dbId");
		String dcId = config.get("job." + platform + ".dcId");
		String jobName = config.get("job." + platform + ".raw.jobName");
		String tableName = config.get("job." + platform + ".raw.table");
		/*
		 * Initialize Utils
		 */
		DocumentDbUtils.init(config);
		TableUtils.init(config);
		/*
		 * 
		 */
		TimeZone tz = TimeZone.getTimeZone("GMT+0");
		Database db = DocumentDbUtils.getDb(dbId);
		String dbLink = db.getSelfLink();
		DocumentCollection dc = DocumentDbUtils.getDC(dbLink, dcId);
		String dcLink = dc.getSelfLink();
		JobEntity job = TableUtils.readJob(jobName);
		String last = job.getLast();
		DateTime cur = new DateTime(last);
		DateTime end = DateTime.now(tz);
		/*
		 * 
		 */
		while (cur.lt(end)) {
			System.out.println(cur);
			DateTime next = cur.plus(0, 0, 0, 1, 0, 0, 0, DayOverflow.Abort);
			/*
			 * 
			 */
			List<Document> docs = DocumentDbUtils.listDocByCreatedAt(dcLink, platform, cur, next, tz);
			for (Document doc : docs) {
				String partitionKey = cur.format("YYYY-MM-DD hh:00:00");
				String rowKey = doc.getId();
				T t = JSON.parseObject(doc.toString(), clazz);
				String json = JSON.toJSONString(t);
				// System.out.println(partitionKey + " - " + rowKey + ":" +
				// json.length() / 1024.);
				DocEntity entity = new DocEntity(partitionKey, rowKey, json);
				TableUtils.writeEntity(tableName, entity);
			}
			/*
			 * 
			 */
			job.setLast(cur.format("YYYY-MM-DD hh:00:00"));
			TableUtils.writeJob(jobName, job);
			cur = next;
		}
	}

	public static void main(String[] args) {
		if (args.length == 2) {
			String filename = args[0];
			String platform = args[1];
			System.out.println(filename + "\t" + platform);
			S3PDataConfig config = new S3PDataConfig(filename);
			switch (platform) {
			case "msdn":
				RawJob.run(config, platform, MSDN.class);
				break;
			case "tn":
				RawJob.run(config, platform, TechNet.class);
				break;
			case "twitter":
				RawJob.run(config, platform, Twitter.class);
				break;
			case "su":
				RawJob.run(config, platform, SuperUser.class);
				break;
			case "sf":
				RawJob.run(config, platform, ServerFault.class);
				break;
			case "so":
				RawJob.run(config, platform, StackOverflow.class);
				break;
			}
		}
	}

}
