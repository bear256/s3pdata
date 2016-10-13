package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.ImpactSummaryService;

public class GetImpactSummary extends Controller {

	private ImpactSummaryService service = new ImpactSummaryService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		int days = getParaToInt("days", 7);
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), days);
		renderJson(json);
	}
}
