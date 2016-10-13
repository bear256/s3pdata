package s3p.data.twitter.weekly;

import java.util.List;

import hirondelle.date4j.DateTime;
import s3p.data.storage.table.DocEntity;

public interface Weekly {

	public Weekly merge(List<DocEntity> listByPN);
	
	public void save(String tableName, DateTime dt, String topic, String pn);
}
