package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.SubPageVoCDetailsService;

public class GetSubPageVoCDetails extends Controller {

	private SubPageVoCDetailsService service = new SubPageVoCDetailsService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		Long date = getParaToLong("Date");
		String pnScope = getPara("PNScope");
		String json = service.get4SubPage(platform.toLowerCase(), topic.toUpperCase(), date, pnScope.toUpperCase());
		renderJson(json);
	}
}
