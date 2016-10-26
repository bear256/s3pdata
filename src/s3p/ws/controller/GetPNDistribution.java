package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.PNDistributionService;

public class GetPNDistribution extends Controller {
	
	private PNDistributionService service = new PNDistributionService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		long date = getParaToLong("date", 0L);
		String datetype = getPara("datetype", "w");
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), date, datetype);
		renderJson(json);
	}
}
