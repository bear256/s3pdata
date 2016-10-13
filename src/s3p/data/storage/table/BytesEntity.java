package s3p.data.storage.table;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class BytesEntity extends TableServiceEntity {

	private byte[] json;

	public BytesEntity() {
		super();
	}

	public BytesEntity(String partitionKey, String rowKey) {
		super(partitionKey, rowKey);
	}

	public BytesEntity(String partitionKey, String rowKey, byte[] json) {
		super(partitionKey, rowKey);
		this.json = json;
	}

	public byte[] getJson() {
		return json;
	}

	public void setJson(byte[] json) {
		this.json = json;
	}
}
