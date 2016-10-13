package s3p.data.documentdb.msforum;

import org.msgpack.annotation.Message;

@Message
public class CreatedBy {

	private String userId;
	private String url;
	private String displayName;

	public CreatedBy() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

}
