package s3p.data.endpoint.topusers;

import org.msgpack.annotation.Message;

import com.alibaba.fastjson.JSON;

import s3p.data.endpoint.common.VocInfluence;
import s3p.data.endpoint.common.attachedobject.TwitterAttachedObject;

@Message
public class TwitterTopUser {

	private String __type = "VoCInfluenceWithScopeOfCommunityUsere2tbI3aF:#ModelEntities.Reporting";
	private TwitterAttachedObject attachedobject;
	private VocInfluence vocinfluence;

	public TwitterTopUser() {
		super();
		// TODO Auto-generated constructor stub
	}

	public TwitterTopUser(TwitterAttachedObject attachedObject) {
		this.attachedobject = attachedObject;
		this.vocinfluence = new VocInfluence();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		vocinfluence.incVocInfluence(sentiment, influenceCount);
	}

	public String get__type() {
		return __type;
	}

	public void set__type(String __type) {
		this.__type = __type;
	}

	public TwitterAttachedObject getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(TwitterAttachedObject attachedobject) {
		this.attachedobject = attachedobject;
	}

	public VocInfluence getVocinfluence() {
		return vocinfluence;
	}

	public void setVocinfluence(VocInfluence vocinfluence) {
		this.vocinfluence = vocinfluence;
	}

	public String toString() {
		return JSON.toJSONString(this);
	}

}
