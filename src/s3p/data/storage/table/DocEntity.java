package s3p.data.storage.table;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class DocEntity extends TableServiceEntity {

	private String json;

	public DocEntity() {
		super();
	}

	public DocEntity(String partitionKey, String rowKey) {
		super(partitionKey, rowKey);
	}

	public DocEntity(String partitionKey, String rowKey, String json) {
		super(partitionKey, rowKey);
		this.json = json;
	}

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}
}
