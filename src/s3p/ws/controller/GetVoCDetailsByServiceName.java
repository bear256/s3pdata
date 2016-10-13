package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.VoCDetailsService;

public class GetVoCDetailsByServiceName extends Controller {

	private VoCDetailsService service = new VoCDetailsService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		String serviceName = getPara("servicename");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String json = service.getByServiceName(platform.toLowerCase(), topic.toUpperCase(), serviceName,
				pnScope.toUpperCase(), days);
		renderJson(json);
	}
}
