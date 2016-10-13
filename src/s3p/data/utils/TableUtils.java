package s3p.data.utils;

import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableServiceEntity;

import hirondelle.date4j.DateTime;
import hirondelle.date4j.DateTime.DayOverflow;
import s3p.data.config.S3PDataConfig;
import s3p.data.storage.table.BytesEntity;
import s3p.data.storage.table.DocEntity;
import s3p.data.storage.table.JobEntity;

public class TableUtils {

	private static CloudTableClient client = null;

	public static void init() {
		CloudStorageAccount account = CloudStorageAccount.getDevelopmentStorageAccount();
		client = account.createCloudTableClient();
	}

	public static void init(S3PDataConfig config) {
		String account = config.get("storage.account");
		String key = config.get("storage.key");
		String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s",
				account, key);
		if (client == null) {
			try {
				CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
				client = storageAccount.createCloudTableClient();
			} catch (InvalidKeyException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}

	public static CloudTable getTable(String tableName) {
		CloudTable table = null;
		try {
			table = client.getTableReference(tableName);
			table.createIfNotExists();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
		return table;
	}

	public static JobEntity readJob(String jobName) {
		CloudTable table = getTable("Job");
		String filter = String.format("PartitionKey eq '%s'", jobName);
		TableQuery<JobEntity> query = TableQuery.from(JobEntity.class).where(filter);
		Iterator<JobEntity> iterator = table.execute(query).iterator();
		if (iterator.hasNext()) {
			return iterator.next();
		} else {
			return null;
		}
	}

	public static void writeJob(String jobName, JobEntity job) {
		CloudTable table = getTable("Job");
		TableOperation operation = TableOperation.insertOrReplace(job);
		try {
			table.execute(operation);
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

	// public static List<DocEntity> readDocs2(String tableName, String
	// partitionKey) {
	// CloudTable table = getTable(tableName);
	// String filter = String.format("PartitionKey eq '%s'", partitionKey);
	// TableQuery<DocEntity> query =
	// TableQuery.from(DocEntity.class).where(filter);
	// Iterator<DocEntity> iterator = table.execute(query).iterator();
	// List<DocEntity> list = new ArrayList<>();
	// while (iterator.hasNext()) {
	// DocEntity entity = iterator.next();
	// list.add(entity);
	// }
	// return list;
	// }

	public static List<DocEntity> readDocs(String tableName, String partitionKey) {
		CloudTable table = getTable(tableName);
		String filter = String.format("PartitionKey eq '%s'", partitionKey);
		TableQuery<BytesEntity> query = TableQuery.from(BytesEntity.class).where(filter);
		Iterator<BytesEntity> iterator = table.execute(query).iterator();
		List<DocEntity> list = new ArrayList<>();
		while (iterator.hasNext()) {
			BytesEntity entity = iterator.next();
			String entityPartitionKey = entity.getPartitionKey();
			String entityRowKey = entity.getRowKey();
			String json = new String(entity.getJson(), Charset.forName("UTF-8"));
			DocEntity doc = new DocEntity(entityPartitionKey, entityRowKey, json);
			list.add(doc);
		}
		return list;
	}

	public static void writeEntity(String tableName, TableServiceEntity entity) {
		CloudTable table = getTable(tableName);
		DocEntity doc = (DocEntity) entity;
		String partitionKey = doc.getPartitionKey();
		String rowKey = doc.getRowKey();
		String json = doc.getJson();
		BytesEntity bytes = new BytesEntity(partitionKey, rowKey, json.getBytes(Charset.forName("UTF-8")));
		TableOperation operation = TableOperation.insertOrReplace(bytes);
		try {
			table.execute(operation);
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

	public static List<DocEntity> filterDocs(String tableName, String partitionKey, String rowKey1, String rowKey2) {
		CloudTable table = getTable(tableName);
		String filter = String.format("(PartitionKey eq '%s') and (RowKey ge '%s') and (RowKey le '%s')", partitionKey,
				rowKey1, rowKey2);
		TableQuery<BytesEntity> query = TableQuery.from(BytesEntity.class).where(filter);
		Iterator<BytesEntity> iterator = table.execute(query).iterator();
		List<DocEntity> list = new ArrayList<>();
		while (iterator.hasNext()) {
			BytesEntity entity = iterator.next();
			String entityPartitionKey = entity.getPartitionKey();
			String entityRowKey = entity.getRowKey();
			String json = new String(entity.getJson(), Charset.forName("UTF-8"));
			DocEntity doc = new DocEntity(entityPartitionKey, entityRowKey, json);
			list.add(doc);
		}
		return list;
	}

	public static void convertDoc2Bytes(String tableName, String partitionKey) {
		CloudTable table = getTable(tableName);
		String filter = String.format("PartitionKey eq '%s'", partitionKey);
		TableQuery<DocEntity> query = TableQuery.from(DocEntity.class).where(filter);
		Iterator<DocEntity> iterator = table.execute(query).iterator();
		while (iterator.hasNext()) {
			DocEntity entity = iterator.next();
			if (entity.getJson() != null) {
				System.out
						.println("Convert: " + tableName + " " + entity.getPartitionKey() + " - " + entity.getRowKey());
				writeEntity(tableName, entity);
			} else {
				System.out.println("Had been converted: " + tableName + " " + entity.getPartitionKey() + " - "
						+ entity.getRowKey());
			}
		}
	}

	public static void main(String[] args) {
		S3PDataConfig config = new S3PDataConfig();
		TableUtils.init(config);
		// for (DocEntity doc : readDocs("TwitterRaw", "2016-08-18 05:00:00")) {
		// System.out.println(doc.getJson());
		// }
		String platform = "Twitter";
		String raw = platform + "Raw";
		String hourly = platform + "Hourly";
		String daily = platform + "Daily";
		String weekly = platform + "Weekly";
		DateTime start = new DateTime("2016-08-31 23:00:00");
		DateTime end = new DateTime("2016-09-01 00:00:00");
		for (DateTime cur = start; cur.lteq(end); cur = cur.plus(0, 0, 0, 1, 0, 0, 0, DayOverflow.Abort)) {
			System.out.println("RAW: " + cur.format("YYYY-MM-DD hh:00:00"));
			convertDoc2Bytes(raw, cur.format("YYYY-MM-DD hh:00:00"));
			if ("23".equals(cur.format("hh"))) {
				System.out.println("Hourly & Daily: " + cur.format("YYYY-MM-DD"));
				convertDoc2Bytes(hourly, cur.format("YYYY-MM-DD"));
				convertDoc2Bytes(daily, cur.format("YYYY-MM-DD"));
				// if (start.numDaysFrom(cur) >= 6) {
				System.out.println("Weekly: " + cur.format("YYYY-MM-DD"));
				convertDoc2Bytes(weekly, cur.format("YYYY-MM-DD"));
				// }
			}
		}
	}

}
