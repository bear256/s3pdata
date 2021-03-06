package s3p.data.endpoint.regiondistribution;

import s3p.data.endpoint.common.VocInfluence;

public class RegionDistribution {

	private String attachedobject;
	private VocInfluence vocinfluence;

	public RegionDistribution() {
		super();
	}

	public RegionDistribution(String attachedobject) {
		super();
		this.attachedobject = attachedobject;
		this.vocinfluence = new VocInfluence();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		vocinfluence.incVocInfluence(sentiment, influenceCount);
	}

	public String getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(String attachedobject) {
		this.attachedobject = attachedobject;
	}

	public VocInfluence getVocinfluence() {
		return vocinfluence;
	}

	public void setVocinfluence(VocInfluence vocinfluence) {
		this.vocinfluence = vocinfluence;
	}

}
