package s3p.data.endpoint.volspikes;

import s3p.data.endpoint.common.VocInfluence;
import s3p.data.endpoint.common.attachedobject.VolSpikeAttachedObject;

public class UserRegionVolSpike {

	private VolSpikeAttachedObject attachedobject;
	private VocInfluence vocinfluence;

	public UserRegionVolSpike() {
		super();
		this.attachedobject = new VolSpikeAttachedObject();
		this.vocinfluence = new VocInfluence();
	}

	public UserRegionVolSpike(VolSpikeAttachedObject attachedobject) {
		super();
		this.attachedobject = attachedobject;
		this.vocinfluence = new VocInfluence();
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
