package s3p.data.endpoint.impactsummary;

import s3p.data.endpoint.common.VocInfluence;

public class VocInsights {

	private double comparedratio;
	private Integer detectedhourlyspikesvol;
	private VocInfluence objectcountlasttime;
	private VocInfluence objectcountthistime;

	public VocInsights() {
		super();
		// TODO Auto-generated constructor stub
	}

	public VocInsights(double comparedratio, Integer detectedhourlyspikesvol, VocInfluence objectcountlasttime,
			VocInfluence objectcountthistime) {
		super();
		this.comparedratio = comparedratio;
		this.detectedhourlyspikesvol = detectedhourlyspikesvol;
		this.objectcountlasttime = objectcountlasttime;
		this.objectcountthistime = objectcountthistime;
	}

	public double getComparedratio() {
		return comparedratio;
	}

	public void setComparedratio(double comparedratio) {
		this.comparedratio = comparedratio;
	}

	public Integer getDetectedhourlyspikesvol() {
		return detectedhourlyspikesvol;
	}

	public void setDetectedhourlyspikesvol(Integer detectedhourlyspikesvol) {
		this.detectedhourlyspikesvol = detectedhourlyspikesvol;
	}

	public VocInfluence getObjectcountlasttime() {
		return objectcountlasttime;
	}

	public void setObjectcountlasttime(VocInfluence objectcountlasttime) {
		this.objectcountlasttime = objectcountlasttime;
	}

	public VocInfluence getObjectcountthistime() {
		return objectcountthistime;
	}

	public void setObjectcountthistime(VocInfluence objectcountthistime) {
		this.objectcountthistime = objectcountthistime;
	}
}
