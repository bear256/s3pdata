package s3p.data.endpoint.impactsummary;

public class RegionOfUsers {

	private double comparedratio;
	private Integer detectedhourlyspikesvol;
	private Integer objectcountlasttime;
	private Integer objectcountthistime;

	public RegionOfUsers() {
		super();
		// TODO Auto-generated constructor stub
	}

	public RegionOfUsers(double comparedratio, Integer detectedhourlyspikesvol, Integer objectcountlasttime,
			Integer objectcountthistime) {
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

	public Integer getObjectcountlasttime() {
		return objectcountlasttime;
	}

	public void setObjectcountlasttime(Integer objectcountlasttime) {
		this.objectcountlasttime = objectcountlasttime;
	}

	public Integer getObjectcountthistime() {
		return objectcountthistime;
	}

	public void setObjectcountthistime(Integer objectcountthistime) {
		this.objectcountthistime = objectcountthistime;
	}

}
