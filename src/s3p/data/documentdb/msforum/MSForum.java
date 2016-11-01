package s3p.data.documentdb.msforum;

import org.msgpack.annotation.Message;

import s3p.data.utils.ServiceNameUtils;

@Message
public class MSForum {

	private String id;
	private String title;
	private String url;
	private String webUrl;
	private String type;
	private String state;
	private boolean hasCode;
	private boolean isLocked;
	private Long created;
	private CreatedBy createdBy;
	private Integer answers;
	private Integer proposedAnswers;
	private Integer views;
	private boolean isAbusive;
	private Integer abusiveMessages;
	private boolean isHelpful;
	private Long locked_date;
	private Long lastReply;
	private String lastReplyMessageId;
	private Long lastContentChangeOrAction;
	private LastContentChangeOrActionBy lastContentChangeOrActionBy;
	private Forum forum;
	private Integer votes;
	private String body;
	private String repliesUrl;
	private Integer repliesCount;
	private Integer sentimentscore;
	private QuestionsReply[] questionsReplies;
	private String _rid;
	private String _self;
	private String _etag;
	private String _attachments;
	private Long _ts;

	public MSForum() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getWebUrl() {
		return webUrl;
	}

	public void setWebUrl(String webUrl) {
		this.webUrl = webUrl;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public boolean isHasCode() {
		return hasCode;
	}

	public void setHasCode(boolean hasCode) {
		this.hasCode = hasCode;
	}

	public boolean isLocked() {
		return isLocked;
	}

	public void setLocked(boolean isLocked) {
		this.isLocked = isLocked;
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

	public Integer getAnswers() {
		return answers;
	}

	public void setAnswers(Integer answers) {
		this.answers = answers;
	}

	public Integer getProposedAnswers() {
		return proposedAnswers;
	}

	public void setProposedAnswers(Integer proposedAnswers) {
		this.proposedAnswers = proposedAnswers;
	}

	public Integer getViews() {
		return views;
	}

	public void setViews(Integer views) {
		this.views = views;
	}

	public boolean isAbusive() {
		return isAbusive;
	}

	public void setAbusive(boolean isAbusive) {
		this.isAbusive = isAbusive;
	}

	public Integer getAbusiveMessages() {
		return abusiveMessages;
	}

	public void setAbusiveMessages(Integer abusiveMessages) {
		this.abusiveMessages = abusiveMessages;
	}

	public boolean isHelpful() {
		return isHelpful;
	}

	public void setHelpful(boolean isHelpful) {
		this.isHelpful = isHelpful;
	}

	public Long getLocked_date() {
		return locked_date;
	}

	public void setLocked_date(Long locked_date) {
		this.locked_date = locked_date;
	}

	public Long getLastReply() {
		return lastReply;
	}

	public void setLastReply(Long lastReply) {
		this.lastReply = lastReply;
	}

	public String getLastReplyMessageId() {
		return lastReplyMessageId;
	}

	public void setLastReplyMessageId(String lastReplyMessageId) {
		this.lastReplyMessageId = lastReplyMessageId;
	}

	public Long getLastContentChangeOrAction() {
		return lastContentChangeOrAction;
	}

	public void setLastContentChangeOrAction(Long lastContentChangeOrAction) {
		this.lastContentChangeOrAction = lastContentChangeOrAction;
	}

	public LastContentChangeOrActionBy getLastContentChangeOrActionBy() {
		return lastContentChangeOrActionBy;
	}

	public void setLastContentChangeOrActionBy(LastContentChangeOrActionBy lastContentChangeOrActionBy) {
		this.lastContentChangeOrActionBy = lastContentChangeOrActionBy;
	}

	public Forum getForum() {
		return forum;
	}

	public void setForum(Forum forum) {
		this.forum = forum;
	}

	public Integer getVotes() {
		return votes;
	}

	public void setVotes(Integer votes) {
		this.votes = votes;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getRepliesUrl() {
		return repliesUrl;
	}

	public void setRepliesUrl(String repliesUrl) {
		this.repliesUrl = repliesUrl;
	}

	public Integer getRepliesCount() {
		return repliesCount;
	}

	public void setRepliesCount(Integer repliesCount) {
		this.repliesCount = repliesCount;
	}

	public Integer getSentimentscore() {
		return sentimentscore;
	}

	public void setSentimentscore(Integer sentimentscore) {
		this.sentimentscore = sentimentscore;
	}

	public QuestionsReply[] getQuestionsReplies() {
		return questionsReplies;
	}

	public void setQuestionsReplies(QuestionsReply[] questionsReplies) {
		this.questionsReplies = questionsReplies;
	}

	public String get_rid() {
		return _rid;
	}

	public void set_rid(String _rid) {
		this._rid = _rid;
	}

	public String get_self() {
		return _self;
	}

	public void set_self(String _self) {
		this._self = _self;
	}

	public String get_etag() {
		return _etag;
	}

	public void set_etag(String _etag) {
		this._etag = _etag;
	}

	public String get_attachments() {
		return _attachments;
	}

	public void set_attachments(String _attachments) {
		this._attachments = _attachments;
	}

	public Long get_ts() {
		return _ts;
	}

	public void set_ts(Long _ts) {
		this._ts = _ts;
	}

	public String topic() {
		// String topic = "Unknown";
		// if (ServiceNameUtils.match4MSForum("Azure",
		// forum.getDisplayName()).size() > 0) {
		// topic = "Azure";
		// }
		String topic = "Azure";
		return topic;
	}

	public String userId() {
		return createdBy != null ? createdBy.getUserId() : "";
	}

}
