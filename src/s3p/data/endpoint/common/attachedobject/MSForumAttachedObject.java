package s3p.data.endpoint.common.attachedobject;

import org.msgpack.annotation.Message;

import s3p.data.documentdb.msforum.CreatedBy;

@Message
public class MSForumAttachedObject {

	private String userId;
	private String url;
	private String displayName;

	public MSForumAttachedObject() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MSForumAttachedObject(CreatedBy createdBy) {
		this.userId = createdBy.getUserId();
		this.url = createdBy.getUrl();
		this.displayName = createdBy.getDisplayName();
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
