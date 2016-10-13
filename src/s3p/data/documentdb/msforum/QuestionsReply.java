package s3p.data.documentdb.msforum;

import org.msgpack.annotation.Message;

@Message
public class QuestionsReply {

	private String id;
	private String parentId;
	private Long created;
	private CreatedBy createdBy;
	private boolean isAbusive;
	private boolean isAnswer;
	private String body;
	private Long lastModified;
	private int votes;
	private int sentimentscore;

	public QuestionsReply() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public Long getCreated() {
		return created;
	}

	public void setCreated(Long created) {
		this.created = created;
	}

	public CreatedBy getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(CreatedBy createdBy) {
		this.createdBy = createdBy;
	}

	public boolean isAbusive() {
		return isAbusive;
	}

	public void setAbusive(boolean isAbusive) {
		this.isAbusive = isAbusive;
	}

	public boolean isAnswer() {
		return isAnswer;
	}

	public void setAnswer(boolean isAnswer) {
		this.isAnswer = isAnswer;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public Long getLastModified() {
		return lastModified;
	}

	public void setLastModified(Long lastModified) {
		this.lastModified = lastModified;
	}

	public int getVotes() {
		return votes;
	}

	public void setVotes(int votes) {
		this.votes = votes;
	}

	public int getSentimentscore() {
		return sentimentscore;
	}

	public void setSentimentscore(int sentimentscore) {
		this.sentimentscore = sentimentscore;
	}

}
