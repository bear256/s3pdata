package s3p.data.utils.anomalydetection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestBody {

	private List<String[]> data;
	private Map<String, String> params;

	public RequestBody() {
		this.data = new ArrayList<>();
		data.add(new String[2]);
		initParams();
	}

	public RequestBody(List<String[]> data) {
		this.data = data;
		initParams();
	}

	private void initParams() {
		params = new HashMap<String, String>();
		params.put("tspikedetector.sensitivity", "3");
		params.put("zspikedetector.sensitivity", "3");
		params.put("trenddetector.sensitivity", "3");
		params.put("bileveldetector.sensitivity", "3");
		params.put("postprocess.tailRows", ""+data.size());
	}

	public List<String[]> getData() {
		return data;
	}

	public void setData(List<String[]> data) {
		this.data = data;
	}

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}
}
