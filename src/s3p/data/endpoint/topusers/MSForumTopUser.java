package s3p.data.endpoint.topusers;

import org.msgpack.annotation.Message;

import com.alibaba.fastjson.JSON;

import s3p.data.endpoint.common.VocInfluence;
import s3p.data.endpoint.common.attachedobject.MSForumAttachedObject;

@Message
public class MSForumTopUser {

	private String __type = "VoCInfluenceWithScopeOfCommunityUsere2tbI3aF:#ModelEntities.Reporting";
	private MSForumAttachedObject attachedobject;
	private VocInfluence vocinfluence;

	public MSForumTopUser() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MSForumTopUser(MSForumAttachedObject attachedObject) {
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

	public MSForumAttachedObject getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(MSForumAttachedObject attachedobject) {
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
