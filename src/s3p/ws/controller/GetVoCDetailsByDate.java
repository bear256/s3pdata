package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.VoCDetailsService;

public class GetVoCDetailsByDate extends Controller {

	private VoCDetailsService service = new VoCDetailsService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		Long date = getParaToLong("date");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String datetype = getPara("datetype", "h");
		String json = service.getByDate(platform.toLowerCase(), topic.toUpperCase(), date, pnScope.toUpperCase(), days, datetype);
		renderJson(json);
	}
}
