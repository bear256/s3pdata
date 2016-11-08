package s3p.ws.config;

import java.util.TimeZone;

import com.jfinal.config.Constants;
import com.jfinal.config.Handlers;
import com.jfinal.config.Interceptors;
import com.jfinal.config.JFinalConfig;
import com.jfinal.config.Plugins;
import com.jfinal.config.Routes;
import com.jfinal.core.JFinal;

import hirondelle.date4j.DateTime;
import s3p.data.config.S3PDataConfig;
import s3p.data.utils.TableUtils;
import s3p.ws.controller.DataStatus;
import s3p.ws.controller.GetDailyInfluence;
import s3p.ws.controller.GetDailyVolSpikes;
import s3p.ws.controller.GetImpactSummary;
import s3p.ws.controller.GetInfluenceVolSpikes;
import s3p.ws.controller.GetKeywordsMentionedMostMapping;
import s3p.ws.controller.GetMentionedMostServiceList;
import s3p.ws.controller.GetMentionedMostServiceListByUserVol;
import s3p.ws.controller.GetMentionedMostServiceTrend;
import s3p.ws.controller.GetMessageVolSpikes;
import s3p.ws.controller.GetPNDistribution;
import s3p.ws.controller.GetRegionDistribution;
import s3p.ws.controller.GetSubPageVoCDetails;
import s3p.ws.controller.GetSubPageVoCDetailsByKeywords;
import s3p.ws.controller.GetTopUsers;
import s3p.ws.controller.GetUserRegionVolSpikes;
import s3p.ws.controller.GetUserVolSpikes;
import s3p.ws.controller.GetVoCDetailsByDate;
import s3p.ws.controller.GetVoCDetailsByPN;
import s3p.ws.controller.GetVoCDetailsByServiceName;
import s3p.ws.controller.GetVoCDetailsByUser;
import s3p.ws.utils.ConfigWrapper;

public class S3PWSConfig extends JFinalConfig {

	@Override
	public void configConstant(Constants me) {
		System.out.println(DateTime.now(TimeZone.getTimeZone("GMT+0")));
		loadPropertyFile("s3pdata.properties");
		me.setDevMode(getPropertyToBoolean("devMode", true));
		S3PDataConfig config = new S3PDataConfig();
		ConfigWrapper.init(config);
		TableUtils.init(config);
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true"); 
	}

	@Override
	public void configHandler(Handlers me) {

	}

	@Override
	public void configInterceptor(Interceptors me) {

	}

	@Override
	public void configPlugin(Plugins me) {

	}

	@Override
	public void configRoute(Routes me) {
		me.add("/GetTopUsers", GetTopUsers.class);
		me.add("/GetPNDistribution", GetPNDistribution.class);
		me.add("/GetRegionDistribution", GetRegionDistribution.class);
		me.add("/GetDailyInfluence", GetDailyInfluence.class);
		me.add("/GetMessageVolSpikes", GetMessageVolSpikes.class);
		me.add("/GetInfluenceVolSpikes", GetInfluenceVolSpikes.class);
		me.add("/GetUserVolSpikes", GetUserVolSpikes.class);
		me.add("/GetUserRegionVolSpikes", GetUserRegionVolSpikes.class);
		me.add("/GetDailyVolSpikes", GetDailyVolSpikes.class);
		me.add("/GetMentionedMostServiceList", GetMentionedMostServiceList.class);
		me.add("/GetMentionedMostServiceTrend", GetMentionedMostServiceTrend.class);
		me.add("/GetMentionedMostServiceListByUserVol", GetMentionedMostServiceListByUserVol.class);
		me.add("/GetKeywordsMentionedMostMapping", GetKeywordsMentionedMostMapping.class);
		me.add("/GetVoCDetailsByUser", GetVoCDetailsByUser.class);
		me.add("/GetVoCDetailsByDate", GetVoCDetailsByDate.class);
		me.add("/GetVoCDetailsByPN", GetVoCDetailsByPN.class);
		me.add("/GetVoCDetailsByServiceName", GetVoCDetailsByServiceName.class);
		me.add("/GetSubPageVoCDetails", GetSubPageVoCDetails.class);
		me.add("/GetSubPageVoCDetailsByKeywords", GetSubPageVoCDetailsByKeywords.class);
		me.add("/GetImpactSummary", GetImpactSummary.class);
		//
		me.add("/datastatus", DataStatus.class);
	}

	public static void main(String[] args) {
		String httpPlatformPort = System.getenv("HTTP_PLATFORM_PORT");
		int port = 8080;
		if (httpPlatformPort != null) {
			port = Integer.parseInt(httpPlatformPort);
		}
		JFinal.start("WebContent", port, "/", 5);
	}

}
