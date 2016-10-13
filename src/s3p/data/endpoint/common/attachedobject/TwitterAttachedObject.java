package s3p.data.endpoint.common.attachedobject;

import org.msgpack.annotation.Message;

import s3p.data.documentdb.twitter.User;

@Message
public class TwitterAttachedObject {

	private Long created_at;
	private String description;
	private Integer followers_count;
	private Integer friends_count;
	private Long id;
	private String id_str;
	private String location;
	private String name;
	private String profile_image_url;
	private String screen_name;
	private Integer statuses_count;
	private String time_zone;

	public TwitterAttachedObject() {
		super();
		// TODO Auto-generated constructor stub
	}

	public TwitterAttachedObject(User user) {
		this.created_at = user.getCreated_at();
		this.description = user.getDescription();
		this.followers_count = user.getFollowers_count();
		this.friends_count = user.getFriends_count();
		this.id = user.getId();
		this.id_str = user.getId_str();
		this.location = user.getLocation();
		this.name = user.getName();
		this.profile_image_url = user.getProfile_image_url();
		this.screen_name = user.getScreen_name();
		this.statuses_count = user.getStatuses_count();
		this.time_zone = user.getTime_zone();
	}

	public Long getCreated_at() {
		return created_at;
	}

	public void setCreated_at(Long created_at) {
		this.created_at = created_at;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getFollowers_count() {
		return followers_count;
	}

	public void setFollowers_count(Integer followers_count) {
		this.followers_count = followers_count;
	}

	public Integer getFriends_count() {
		return friends_count;
	}

	public void setFriends_count(Integer friends_count) {
		this.friends_count = friends_count;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getId_str() {
		return id_str;
	}

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProfile_image_url() {
		return profile_image_url;
	}

	public void setProfile_image_url(String profile_image_url) {
		this.profile_image_url = profile_image_url;
	}

	public String getScreen_name() {
		return screen_name;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public Integer getStatuses_count() {
		return statuses_count;
	}

	public void setStatuses_count(Integer statuses_count) {
		this.statuses_count = statuses_count;
	}

	public String getTime_zone() {
		return time_zone;
	}

	public void setTime_zone(String time_zone) {
		this.time_zone = time_zone;
	}

}
