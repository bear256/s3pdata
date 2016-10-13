package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.RegionDistributionService;

public class GetRegionDistribution extends Controller {
	
	private RegionDistributionService service = new RegionDistributionService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days");
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), days);
		renderJson(json);
	}
}
