package s3p.data.storage.table;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class JobEntity extends TableServiceEntity {

	private String start;
	private String last;

	public JobEntity() {
		super();
		// TODO Auto-generated constructor stub
	}

	public JobEntity(String partitionKey, String rowKey) {
		super(partitionKey, rowKey);
		// TODO Auto-generated constructor stub
	}

	public JobEntity(String partitionKey) {
		super(partitionKey, "status");
		// TODO Auto-generated constructor stub
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getLast() {
		return last;
	}

	public void setLast(String last) {
		this.last = last;
	}

}
