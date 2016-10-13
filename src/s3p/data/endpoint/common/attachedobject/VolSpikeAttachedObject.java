package s3p.data.endpoint.common.attachedobject;

public class VolSpikeAttachedObject {

	private boolean isspike = false;
	private Long timeslot;

	public VolSpikeAttachedObject() {
		super();
	}

	public VolSpikeAttachedObject(boolean isspike, Long timeslot) {
		super();
		this.isspike = isspike;
		this.timeslot = timeslot;
	}

	public boolean isIsspike() {
		return isspike;
	}

	public void setIsspike(boolean isspike) {
		this.isspike = isspike;
	}

	public Long getTimeslot() {
		return timeslot;
	}

	public void setTimeslot(Long timeslot) {
		this.timeslot = timeslot;
	}

}
