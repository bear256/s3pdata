package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.VoCDetailsService;

public class GetVoCDetailsByPN extends Controller {

	private VoCDetailsService service = new VoCDetailsService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String json = service.getByPN(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), days);
		renderJson(json);
	}
}
