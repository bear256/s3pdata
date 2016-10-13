package s3p.data.utils;

import java.util.List;
import java.util.TimeZone;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;

import hirondelle.date4j.DateTime;
import s3p.data.config.S3PDataConfig;

public class DocumentDbUtils {

	private static DocumentClient client = null;

	public static void init(S3PDataConfig config) {
		String uri = config.get("documentdb.uri");
		String key = config.get("documentdb.key");
		if (client == null) {
			client = new DocumentClient(uri, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
		}
	}

	public static Database getDb(String dbId) {
		String query = String.format("SELECT * from ROOT r WHERE r.id = '%s'", dbId);
		FeedOptions options = null;
		List<Database> dbs = client.queryDatabases(query, options).getQueryIterable().toList();
		return dbs.size() != 0 ? dbs.get(0) : null;
	}

	public static String getDbLink(String dbId) {
		Database db = getDb(dbId);
		return db != null ? db.getSelfLink() : null;
	}

	public static DocumentCollection getDC(String dbIdOrLink, String dcId) {
		String dbLink = dbIdOrLink;
		if (!dbIdOrLink.startsWith("dbs")) {
			String dbId = dbIdOrLink;
			dbLink = getDbLink(dbId);
		}
		String query = String.format("SELECT * from ROOT r WHERE r.id = '%s'", dcId);
		FeedOptions options = null;
		List<DocumentCollection> dcs = client.queryCollections(dbLink, query, options).getQueryIterable().toList();
		return dcs.size() > 0 ? dcs.get(0) : null;
	}

	public static String getDCLink(String dbId, String dcId) {
		DocumentCollection dc = getDC(dbId, dcId);
		return dc != null ? dc.getSelfLink() : null;
	}

	public static Document getDocByCond(String dcLink, String cond) {
		String query = String.format("SELECT TOP 1 * from ROOT r %s", cond != null ? "WHERE " + cond : "");
		FeedOptions options = null;
		List<Document> docs = client.queryDocuments(dcLink, query, options).getQueryIterable().toList();
		return docs.size() > 0 ? docs.get(0) : null;
	}

	public static Document firstDoc(String dcLink, String platform, boolean reverse) {
		String property = null;
		switch(platform) {
		case "msdn":
			property = "created";
			break;
		case "tn":
			property = "created";
			break;
		case "twitter":
			property = "created_at";
			break;
		case "su":
			property = "creation_date";
			break;
		case "sf":
			property = "creation_date";
			break;
		case "so":
			property = "creation_date";
			break;
		}
		String query = String.format("SELECT TOP 1 * from ROOT r %s", reverse ? "ORDER BY r."+property+" DESC" : "");
		FeedOptions options = null;
		List<Document> docs = client.queryDocuments(dcLink, query, options).getQueryIterable().toList();
		return docs.size() > 0 ? docs.get(0) : null;
	}

	public static Document firstDoc(String dcLink, String platform) {
		return firstDoc(dcLink, platform, false);
	}

	public static Document lastDoc(String dcLink, String platform) {
		return firstDoc(dcLink, platform, true);
	}

	public static List<Document> listDocByCreatedAt(String dcLink, String platform, DateTime start, DateTime end, TimeZone tz) {
		Long startime = start.getMilliseconds(tz) / 1000;
		Long endtime = end.getMilliseconds(tz) / 1000;
		String query = null;
		switch(platform) {
		case "msdn":
			query = String.format("SELECT * from ROOT r WHERE r.created >= '%s' and r.created < '%s'",
					startime, endtime);
			break;
		case "tn":
			query = String.format("SELECT * from ROOT r WHERE r.created >= '%s' and r.created < '%s'",
					startime, endtime);
			break;
		case "twitter":
			query = String.format("SELECT * from ROOT r WHERE r.created_at >= '%s' and r.created_at < '%s'",
					startime, endtime);
			break;
		case "su":
			query = String.format("SELECT * from ROOT r WHERE r.creation_date >= %s and r.creation_date < %s",
					startime, endtime);
			break;
		case "sf":
			query = String.format("SELECT * from ROOT r WHERE r.creation_date >= %s and r.creation_date < %s",
					startime, endtime);
			break;
		case "so":
			query = String.format("SELECT * from ROOT r WHERE r.creation_date >= %s and r.creation_date < %s",
					startime, endtime);
			break;
		}
		FeedOptions options = null;
		List<Document> docs = client.queryDocuments(dcLink, query, options).getQueryIterable().toList();
		return docs;
	}

}
