package s3p.data.endpoint.volspikes;

import s3p.data.endpoint.common.VocInfluence;
import s3p.data.endpoint.common.attachedobject.VolSpikeAttachedObject;

public class MessageVolSpike {

	private VolSpikeAttachedObject attachedobject;
	private VocInfluence vocinfluence;

	public MessageVolSpike() {
		super();
		this.attachedobject = new VolSpikeAttachedObject();
		this.vocinfluence = new VocInfluence();
	}

	public MessageVolSpike(VolSpikeAttachedObject attachedobject) {
		super();
		this.attachedobject = attachedobject;
		this.vocinfluence = new VocInfluence();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		vocinfluence.incVocInfluence(sentiment, influenceCount);
	}

	public VolSpikeAttachedObject getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(VolSpikeAttachedObject attachedobject) {
		this.attachedobject = attachedobject;
	}

	public VocInfluence getVocinfluence() {
		return vocinfluence;
	}

	public void setVocinfluence(VocInfluence vocinfluence) {
		this.vocinfluence = vocinfluence;
	}

}
