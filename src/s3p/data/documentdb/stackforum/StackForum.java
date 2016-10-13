package s3p.data.documentdb.stackforum;

import java.util.ArrayList;
import java.util.List;

import org.msgpack.annotation.Message;

@Message
public class StackForum {

	private Long accepted_answer_id;
	private Integer answer_count;
	private String answers;
	private String body;
	private Integer bounty_amount;
	private Long bounty_closes_date;
	private Long closed_date;
	private String closed_reason;
	private Comment[] comments;
	private Long community_owned_date;
	private Long creation_date;
	private Integer down_vote_count;
	private Integer favorite_count;
	private boolean is_answered;
	private Long last_activity_date;
	private Long last_edit_date;
	private String link;
	private Long locked_date;
	private Owner owner;
	private Long protected_date;
	private Long question_id;
	private Integer score;
	private String[] tags;
	private String title;
	private Integer up_vote_count;
	private Integer view_count;
	private Integer sentimentscore;
	private String id;
	private String _rid;
	private String _self;
	private String _etag;
	private String _attachments;
	private Long _ts;

	public StackForum() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Long getAccepted_answer_id() {
		return accepted_answer_id;
	}

	public void setAccepted_answer_id(Long accepted_answer_id) {
		this.accepted_answer_id = accepted_answer_id;
	}

	public Integer getAnswer_count() {
		return answer_count;
	}

	public void setAnswer_count(Integer answer_count) {
		this.answer_count = answer_count;
	}

	public String getAnswers() {
		return answers;
	}

	public void setAnswers(String answers) {
		this.answers = answers;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public Integer getBounty_amount() {
		return bounty_amount;
	}

	public void setBounty_amount(Integer bounty_amount) {
		this.bounty_amount = bounty_amount;
	}

	public Long getBounty_closes_date() {
		return bounty_closes_date;
	}

	public void setBounty_closes_date(Long bounty_closes_date) {
		this.bounty_closes_date = bounty_closes_date;
	}

	public Long getClosed_date() {
		return closed_date;
	}

	public void setClosed_date(Long closed_date) {
		this.closed_date = closed_date;
	}

	public String getClosed_reason() {
		return closed_reason;
	}

	public void setClosed_reason(String closed_reason) {
		this.closed_reason = closed_reason;
	}

	public Comment[] getComments() {
		return comments;
	}

	public void setComments(Comment[] comments) {
		this.comments = comments;
	}

	public Long getCommunity_owned_date() {
		return community_owned_date;
	}

	public void setCommunity_owned_date(Long community_owned_date) {
		this.community_owned_date = community_owned_date;
	}

	public Long getCreation_date() {
		return creation_date;
	}

	public void setCreation_date(Long creation_date) {
		this.creation_date = creation_date;
	}

	public Integer getDown_vote_count() {
		return down_vote_count;
	}

	public void setDown_vote_count(Integer down_vote_count) {
		this.down_vote_count = down_vote_count;
	}

	public Integer getFavorite_count() {
		return favorite_count;
	}

	public void setFavorite_count(Integer favorite_count) {
		this.favorite_count = favorite_count;
	}

	public boolean isIs_answered() {
		return is_answered;
	}

	public void setIs_answered(boolean is_answered) {
		this.is_answered = is_answered;
	}

	public Long getLast_activity_date() {
		return last_activity_date;
	}

	public void setLast_activity_date(Long last_activity_date) {
		this.last_activity_date = last_activity_date;
	}

	public Long getLast_edit_date() {
		return last_edit_date;
	}

	public void setLast_edit_date(Long last_edit_date) {
		this.last_edit_date = last_edit_date;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public Long getLocked_date() {
		return locked_date;
	}

	public void setLocked_date(Long locked_date) {
		this.locked_date = locked_date;
	}

	public Owner getOwner() {
		return owner;
	}

	public void setOwner(Owner owner) {
		this.owner = owner;
	}

	public Long getProtected_date() {
		return protected_date;
	}

	public void setProtected_date(Long protected_date) {
		this.protected_date = protected_date;
	}

	public Long getQuestion_id() {
		return question_id;
	}

	public void setQuestion_id(Long question_id) {
		this.question_id = question_id;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

	public String[] getTags() {
		return tags;
	}

	public void setTags(String[] tags) {
		this.tags = tags;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Integer getUp_vote_count() {
		return up_vote_count;
	}

	public void setUp_vote_count(Integer up_vote_count) {
		this.up_vote_count = up_vote_count;
	}

	public Integer getView_count() {
		return view_count;
	}

	public void setView_count(Integer view_count) {
		this.view_count = view_count;
	}

	public Integer getSentimentscore() {
		return sentimentscore;
	}

	public void setSentimentscore(Integer sentimentscore) {
		this.sentimentscore = sentimentscore;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public List<String> topics() {
		List<String> topics = new ArrayList<>();
		for (String tag : tags) {
			if (tag.toLowerCase().equals("Azure".toLowerCase())) {
				topics.add("Azure");
			}
			if (tag.toLowerCase().equals("UWP".toLowerCase())) {
				topics.add("UWP");
			}
		}
		return topics;
	}

	public String userId() {
		return owner != null ? owner.getUser_id() + "" : "";
	}
}
