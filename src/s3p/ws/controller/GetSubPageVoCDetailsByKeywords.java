package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.SubPageVoCDetailsService;

public class GetSubPageVoCDetailsByKeywords extends Controller {

	private SubPageVoCDetailsService service = new SubPageVoCDetailsService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		String keywords = getPara("keywords");
		String pnScope = getPara("PNScope");
		boolean isFuzzyQuery = getParaToBoolean("IsFuzzyQuery");
		int days = getParaToInt("days");
		String json = service.get4SubPageByKeywords(platform.toLowerCase(), topic.toUpperCase(), keywords,
				pnScope.toUpperCase(), isFuzzyQuery, days);
		renderJson(json);
	}
}
