package com.share.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <pre>
 * 项目:ReserveMeeting
 * 描述:日期工具类
 * 说明:[例如:公元2016年07月31日 上午 10时15分15秒361毫秒 CST+0800(当前日期为:本年第32周,本月第6周,本年第213天,本月第5星期的星期日)]
 * (
 * yyyy:年,MM:月,dd:日,a:上午/下午[am/pm],HH:时,mm:分,ss:秒,SSS:毫秒,
 * w:年中周数[例如:本年第17周],W:月周数[例如:本月第2周],D:年中天数[例如:本年第168天],
 * F:星期数[例如:2],E:星期文本[例如:星期二],
 * zzz:时区标识[CST/GMT],Z:时区[例如:+0800],G:年限标识(公元)
 * )
 * 作者:ZhangYi
 * 时间:2016年6月23日 上午11:18:01
 * 版本:wrm_v4.0
 * JDK:1.7.80
 * </pre>
 */
public class DateUtil {

	private static Logger logger = LogManager.getLogger(DateUtil.class);

	/**
	 * 日期分隔符(-)
	 */
	public static final String	SPLIT_DATE						= "-";
	/**
	 * 时间分隔符(:)
	 */
	public static final String	SPLIT_TIME						= ":";
	/**
	 * 默认日期时间格式[时间戳(yyyy/MM/dd HH:mm:ss)]
	 */
	public static final String	DEFAULT_ISO_FORMAT_DATE_TIME	= "yyyy/MM/dd HH:mm:ss";
	/**
	 * 默认日期时间格式[时间戳(yyyy-MM-dd HH:mm:ss)]
	 */
	public static final String	DEFAULT_FORMAT_DATE_TIME		= "yyyy-MM-dd HH:mm:ss";
	/**
	 * 默认日期时间格式[日期型(yyyy-MM-dd)]
	 */
	public static final String	DEFAULT_FORMAT_DATE				= "yyyy-MM-dd";
	/**
	 * 默认日期时间格式[日期型(yyyy-MM)]
	 */
	public static final String	DEFAULT_FORMAT_MONTH			= "yyyy-MM";
	/**
	 * 默认日期时间格式[时刻型(HH:mm:ss)]
	 */
	public static final String	DEFAULT_FORMAT_TIME				= "HH:mm:ss";
	/**
	 * 日期时间格式[时间戳(yyyy-MM-dd HH:mm)]
	 */
	public static final String	FORMAT_DATE_TIME				= "yyyy-MM-dd HH:mm";
	/**
	 * 日期时间格式[时间戳(dd HH:mm)]
	 */
	public static final String	FORMAT_PATTERN_DATE_TIME		= "MM-dd HH:mm";
	/**
	 * 日期时间格式[日期型(MM-dd)]
	 */
	public static final String	FORMAT_PATTERN_DATE				= "MM-dd";
	/**
	 * 日期时间格式[时刻型(HH:mm)]
	 */
	public static final String	FORMAT_PATTERN_TIME				= "HH:mm";
	/**
	 * 日期时间格式[时间戳(MM/dd/yyyy)]
	 */
	public static final String	ISO_FORMAT_DATE					= "MM/dd/yyyy";
	/**
	 * 日期时间格式[时间戳(MM/dd/yyyy HH:mm:ss)]
	 */
	public static final String	ISO_FORMAT_DATE_TIME			= "MM/dd/yyyy HH:mm:ss";

	/**
	 * 日期时间格式[时间戳(yyyy-MM-dd'T'HH:mm:ss.SSSZ)]
	 */
	public static final String	UTC_FORMAT_DATE_TIME			= "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	/**
	 * 一天毫秒数
	 */
	public static final long	ONE_DAY							= 1000l * 60 * 60 * 24;

	/**
	 * <pre>
	 * 描述:[日期型]日期转化指定格式字符串
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午3:25:25
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @param format	日期格式(默认:yyyy-MM-dd HH:mm:ss)
	 * @return
	 * </pre>
	 */
	public static Date formatDateTime(String dateTime, String format) {
		try {
			if (StringUtil.isEmpty(format)) format = DEFAULT_FORMAT_DATE_TIME;
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			if (dateTime != null) {
				return dateFormat.parse(dateTime);
			}
		} catch (Exception e) {
			logger.error("--日期转化指定格式[" + format + "]字符串失败!", e);
		}
		return null;
	}

	/**
	 * <pre>
	 * 描述:[字符串型]日期转化指定格式字符串
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午3:25:25
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @param format	日期格式(默认:yyyy-MM-dd HH:mm:ss)
	 * @return
	 * </pre>
	 */
	public static String formatDateTimeStr(Date dateTime, String format) {
		try {
			if (StringUtil.isEmpty(format)) format = DEFAULT_FORMAT_DATE_TIME;
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			if (dateTime != null) {
				return dateFormat.format(dateTime);
			}
		} catch (Exception e) {
			logger.error("--日期转化指定格式[" + format + "]字符串失败!", e);
		}
		return null;
	}

	/**
	 * <pre>
	 * 描述:字符串转日期型(日期格式[yyyy-MM-dd HH:mm:ss])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期字符串(格式:'yyyy-MM-dd HH:mm:ss'或毫秒时间戳值或'yyyy-MM-dd HH:mm')
	 * @return
	 * </pre>
	 */
	public static Date formatDateTime(String dateTime) {
		if (StringUtil.isEmpty(dateTime)) return null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT_DATE_TIME);
			if (NumberUtil.isNumber(dateTime)) {
				Date date = new Date(Long.parseLong(dateTime));
				dateTime = sdf.format(date);
			} else {
				if (!dateTime.contains(":")) {
					dateTime = dateTime + " 00:00:00";
				} else {
					if (dateTime.length() < 19) {
						dateTime = dateTime + ":00";
					}
				}
			}
			return sdf.parse(dateTime);
		} catch (Exception e) {
			logger.error("日期时间格式转换错误：", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:字符串转日期型(日期格式[yyyy-MM-dd])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期字符串(格式:'yyyy-MM-dd HH:mm:ss'或毫秒时间戳值或'yyyy-MM-dd')
	 * @return
	 * </pre>
	 */
	public static Date formatDate(String dateTime) {
		if (StringUtil.isEmpty(dateTime)) return null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT_DATE);
			if (NumberUtil.isNumber(dateTime)) {
				Date date = new Date(Long.parseLong(dateTime));
				dateTime = sdf.format(date);
			}
			return sdf.parse(dateTime);
		} catch (Exception e) {
			logger.error("日期时间格式转换错误：", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:转化指定日期(时分秒置为0)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:14:15
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static Date formatDate(Date dateTime) {
		String startTime = formatDateStr(dateTime);
		dateTime = formatDate(startTime);
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:日期型转字符串(日期格式[yyyy-MM-dd HH:mm:ss])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static String formatDateTimeStr(Date dateTime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT_DATE_TIME);
			return sdf.format(dateTime);
		} catch (Exception e) {
			logger.error("转化日期格式错误：", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:日期型转字符串(日期格式[yyyy-MM-dd HH:mm])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static String formatDateHMTimeStr(Date dateTime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(FORMAT_DATE_TIME);
			return sdf.format(dateTime);
		} catch (Exception e) {
			logger.error("--日期型转字符串(日期格式[yyyy-MM-dd HH:mm])失败!", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:日期型转字符串(日期格式[yyyy-MM-dd])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static String formatDateStr(Date dateTime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT_DATE);
			return sdf.format(dateTime);
		} catch (Exception e) {
			logger.error("--日期型转字符串(日期格式[yyyy-MM-dd])失败!", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:日期型转字符串(日期格式[HH:mm:ss])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static String formatTimeStr(Date dateTime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT_TIME);
			return sdf.format(dateTime);
		} catch (Exception e) {
			logger.error("--日期型转字符串(日期格式[HH:mm:ss])失败!", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:日期型转字符串(日期格式[HH:mm])
	 * 作者:ZhangYi
	 * 时间:2016年4月15日 上午10:34:57
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static String formatHMStr(Date dateTime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(FORMAT_PATTERN_TIME);
			return sdf.format(dateTime);
		} catch (Exception e) {
			logger.error("--日期型转字符串(日期格式[HH:mm])失败!", e);
			return null;
		}
	}

	/**
	 * <pre>
	 * 描述:日期转XMLGregorianCalendar
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:13:36
	 * 参数：(参数列表)
	 * @param date
	 * @return
	 * </pre>
	 */
	public static XMLGregorianCalendar getXMLCalendar(Date date) {
		DatatypeFactory datatypeFactory = null;
		try {
			datatypeFactory = DatatypeFactory.newInstance();
		} catch (DatatypeConfigurationException e) {
			logger.error("获取时间转换工厂时发生错误", e);
		}
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		XMLGregorianCalendar datetime = datatypeFactory.newXMLGregorianCalendar(calendar);
		return datetime;
	}

	/**
	 * <pre>
	 * 描述:日期格式校验(日期格式[yyyy-MM-dd HH:mm:ss])
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:27:42
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @return
	 * </pre>
	 */
	public static boolean isDateTime(String dateTime) {
		if (dateTime == null) return false;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_FORMAT_DATE_TIME);
			sdf.parse(dateTime);
			return true;
		} catch (Exception e) {
			logger.error("日期时间格式校验错误：", e);
			return false;
		}
	}

	/**
	 * <pre>
	 * 描述:间隔指定分钟后日期(例如:每30分钟)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param interval	间隔分钟
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByMinute(Date dateTime, int interval) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.MINUTE, interval);
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定分钟后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:间隔指定小时后日期(例如:每3小时)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param interval	间隔小时
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByHour(Date dateTime, int interval) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.HOUR, interval);
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定小时后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:间隔指定天数后日期(例如:每3天)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param interval	间隔天数
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByDay(Date dateTime, int interval) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.DAY_OF_MONTH, interval);
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定天数后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:间隔指定月数的指定天数后日期(例如:每月1日)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param months	间隔月数(间隔几个月)
	 * @param day		指定天数
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByMonth(Date dateTime, int interval, int day) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.MONTH, interval);
			calendar.set(Calendar.DAY_OF_MONTH, day);
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定月数的指定天数后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:间隔指定月数的指定周数指定星期数后日期(例如:每3个月第一个星期一)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param interval	间隔月数(间隔几个月)
	 * @param num		指定周数(1-4:第几个星期)
	 * @param week		指定周几(1-7:周一至周日,-1:不指定周几(JDK默认星期一))
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByMonth(Date dateTime, int interval, int num, int week) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.MONTH, interval);
			if (num < 0) {// 最后一个星期
				calendar.set(Calendar.DAY_OF_WEEK_IN_MONTH, -1);
			} else {
				calendar.set(Calendar.DAY_OF_WEEK_IN_MONTH, num);
			}
			if (week < 0) {// [默认星期一]
				calendar.set(Calendar.DAY_OF_WEEK, 1 % 7 + 1);
			} else {
				calendar.set(Calendar.DAY_OF_WEEK, week % 7 + 1);
			}
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定月数的指定周数指定星期数后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:间隔指定年数的指定月份指定天数后日期(例如:每年1月1日)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param interval	间隔年(间隔几年)
	 * @param month		指定月份(1-12:月份)
	 * @param day		指定天数
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByYear(Date dateTime, int interval, int month, int day) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.YEAR, interval);
			calendar.set(Calendar.MONTH, month - 1);
			calendar.set(Calendar.DAY_OF_MONTH, day);
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定年数的指定月份指定天数后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:间隔指定年数的指定月份指定周数指定星期数后日期(例如:每年1月份第一个星期一)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:29:07
	 * 参数：(参数列表)
	 * @param dateTime	指定日期
	 * @param interval	间隔年(间隔几年)
	 * @param month		指定月份(1-12:月份)
	 * @param num		指定周数(1-4:第几个星期)
	 * @param week		指定周几(1-7:周一至周日,-1:不指定周几[默认星期一])
	 * @return
	 * </pre>
	 */
	public static Date handleDateTimeByYear(Date dateTime, int interval, int month, int num, int week) {
		try {
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(dateTime);
			calendar.add(Calendar.YEAR, interval);
			calendar.set(Calendar.MONTH, month - 1);
			if (num < 0) {// 最后一个星期
				calendar.set(Calendar.DAY_OF_WEEK_IN_MONTH, -1);
			} else {
				calendar.set(Calendar.DAY_OF_WEEK_IN_MONTH, num);
			}
			if (week < 0) {// [默认星期一]
				calendar.set(Calendar.DAY_OF_WEEK, 1 % 7 + 1);
			} else {
				calendar.set(Calendar.DAY_OF_WEEK, week % 7 + 1);
			}
			dateTime = calendar.getTime();
		} catch (Exception e) {
			logger.error("--间隔指定年数的指定月份指定周数指定星期数后日期异常!", e);
		}
		return dateTime;
	}

	/**
	 * <pre>
	 * 描述:获取当前时间的星期数
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:31:06
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static String formatWeek(Date date) {
		String[] weeks = { "7", "1", "2", "3", "4", "5", "6" };
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
		if (week < 0) week = 0;
		return weeks[week];
	}

	/**
	 * <pre>
	 * 描述:获取中英文星期数
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:32:32
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @param lang	语言(中文:zh/zh_CN,英文:en/en_US)
	 * @return
	 * </pre>
	 */
	public static String formatWeek(Date date, String lang) {
		String[] weeks = { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };
		if (!StringUtil.isEmpty(lang) && (lang.contains("en") || lang.contains("EN"))) {
			weeks = new String[] { "Sun.", "Mon.", "Tues.", "Wed.", "Thur.", "Fri.", "Sat." };
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
		if (week < 0) week = 0;
		return weeks[week];
	}

	/**
	 * <pre>
	 * 描述:获取中英文星期数
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:32:32
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @param lang	语言(中文:zh/zh_CN,英文:en/en_US)
	 * @return
	 * </pre>
	 */
	public static String formatWeek(int week, String lang) {
		String[] weeks = { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };
		if (!StringUtil.isEmpty(lang) && (lang.contains("en") || lang.contains("EN"))) {
			weeks = new String[] { "Sun.", "Mon.", "Tues.", "Wed.", "Thur.", "Fri.", "Sat." };
		}
		if (week < 0) week = 0;
		return weeks[week];
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔分钟数(同一分钟间隔为0)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param from	起始时间
	 * @param to	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalMinutes(String from, String to) {
		try {
			Date startTime = formatDateTime(from);
			Date endTime = formatDateTime(to);
			double interval = (endTime.getTime() - startTime.getTime()) / (double) (1000 * 60);
			return (int) Math.floor(interval);
		} catch (Exception e) {
			logger.error("--获取日期间隔分钟数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔分钟数(同一分钟间隔为0)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param from	起始时间
	 * @param to	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalMinutes(Date startTime, Date endTime) {
		try {
			double interval = (endTime.getTime() - startTime.getTime()) / (double) (1000 * 60);
			return (int) Math.floor(interval);
		} catch (Exception e) {
			logger.error("--获取日期间隔分钟数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔小时数(同一小时间隔为0)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param from	起始时间
	 * @param to	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalHours(String from, String to) {
		try {
			Date startTime = formatDateTime(from);
			Date endTime = formatDateTime(to);
			double interval = (endTime.getTime() - startTime.getTime()) / (double) (1000 * 60 * 60);
			return (int) Math.floor(interval);
		} catch (Exception e) {
			logger.error("--获取日期间隔小时数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔小时数(同一小时间隔为0)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param start	起始时间
	 * @param end	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalHours(Date start, Date end) {
		try {
			double interval = (end.getTime() - start.getTime()) / (double) (1000 * 60 * 60);
			return (int) Math.floor(interval);
		} catch (Exception e) {
			logger.error("--获取日期间隔小时数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔天数(同一天间隔为1)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param from	起始时间
	 * @param to	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalDays(String from, String to) {
		try {
			Date startTime = formatDateTime(from);
			Date endTime = formatDateTime(to);
			double interval = (endTime.getTime() - startTime.getTime()) / (double) (1000 * 60 * 60 * 24);
			return (int) Math.ceil(interval);
		} catch (Exception e) {
			logger.error("--获取日期间隔天数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔天数(同一天间隔为1)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param start	起始时间
	 * @param end	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalDays(Date start, Date end) {
		try {
			double interval = (end.getTime() - start.getTime()) / (double) (1000 * 60 * 60 * 24);
			return (int) Math.ceil(interval);
		} catch (Exception e) {
			logger.error("--获取日期间隔天数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期间隔月数(同一月间隔为0)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:39:08
	 * 参数：(参数列表)
	 * @param start	起始时间
	 * @param end	结束时间
	 * @return
	 * </pre>
	 */
	public static int intervalMonths(Date start, Date end) {
		try {
			int interval = (Integer.valueOf(formatYear(end)) - Integer.valueOf(formatYear(start))) * 12 + ((Integer.valueOf(formatMonth(end)) - Integer.valueOf(formatMonth(start))));
			return interval;
		} catch (Exception e) {
			logger.error("--获取日期间隔月数失败!", e);
		}
		return 0;
	}

	/**
	 * <pre>
	 * 描述:获取日期的日数
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:42:19
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static String formatDay(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("dd");
		String ctime = formatter.format(date);
		return ctime;
	}

	/**
	 * <pre>
	 * 描述:获取日期的月数
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:42:19
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static String formatMonth(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("MM");
		String ctime = formatter.format(date);
		return ctime;
	}

	/**
	 * <pre>
	 * 描述:获取日期的年
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:42:19
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static String formatYear(Date date) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
		String ctime = formatter.format(date);
		return ctime;
	}

	/**
	 * <pre>
	 * 描述:获取指定日期开始时间(格式:yyyy-MM-dd 00:00:00)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatFirstTime(Date date) {
		String dateTime = formatDateStr(date) + " 00:00:00";
		date = formatDateTime(dateTime);
		return date;
	}

	/**
	 * <pre>
	 * 描述:获取指定日期最后时间(格式:yyyy-MM-dd 23:59:59)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatLastTime(Date date) {
		String dateTime = formatDateStr(date) + " 23:59:59";
		return formatDateTime(dateTime);
	}

	/**
	 * <pre>
	 * 描述:获取指定周的第一天(格式:yyyy-MM-dd 00:00:00)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatWeekFirstTime(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_WEEK, 1);
		String dateTime = formatDateStr(calendar.getTime()) + " 00:00:00";
		date = formatDateTime(dateTime);
		return date;
	}

	/**
	 * <pre>
	 * 描述:获取指定周的最后一天(格式:yyyy-MM-dd 23:59:59)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatWeekLastTime(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_WEEK, 7);
		String dateTime = formatDateStr(calendar.getTime()) + " 23:59:59";
		return formatDateTime(dateTime);
	}

	/**
	 * <pre>
	 * 描述:获取指定月的第一天(格式:yyyy-MM-dd 00:00:00)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatMonthFirstTime(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		String dateTime = formatDateStr(calendar.getTime()) + " 00:00:00";
		date = formatDateTime(dateTime);
		return date;
	}

	/**
	 * <pre>
	 * 描述:获取指定月的最后一天(格式:yyyy-MM-dd 23:59:59)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatMonthLastTime(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, 1);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		date.setTime(calendar.getTimeInMillis() - 1 * 24 * 60 * 60 * 1000l);
		String dateTime = formatDateStr(date) + " 23:59:59";
		return formatDateTime(dateTime);
	}

	/**
	 * <pre>
	 * 描述:获取指定年的第一天(格式:yyyy-01-01 00:00:00)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatYearFirstTime(Date date) {
		String dateTime = formatYear(date) + "-01-01 00:00:00";
		return formatDateTime(dateTime);
	}

	/**
	 * <pre>
	 * 描述:获取指定年的最后一天(格式:yyyy-12-31 23:59:59)
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:49:04
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static Date formatYearLastTime(Date date) {
		String dateTime = formatYear(date) + "-12-31 23:59:59";
		return formatDateTime(dateTime);
	}

	/**
	 * <pre>
	 * 描述:获取中文日期字符串
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午4:50:26
	 * 参数：(参数列表)
	 * @param date	指定日期
	 * @return
	 * </pre>
	 */
	public static String formatChinaDate(Date date) {
		String format = "yyyy年MM月dd日";
		return formatDateTimeStr(date, format);
	}

	/**
	 * <pre>
	 * 描述:[字符串型]日期转化指定UTF格式字符串
	 * 作者:ZhangYi
	 * 时间:2016年5月5日 下午3:25:25
	 * 参数：(参数列表)
	 * @param dateTime	日期时间
	 * @param format	日期格式(默认:yyyy-MM-dd'T'HH:mm:ss.SSSZ)
	 * @return
	 * </pre>
	 */
	public static String formatUTCDateTime(Date date, String format) {
		if (StringUtil.isEmpty(format)) format = UTC_FORMAT_DATE_TIME;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int zoneOffset = calendar.get(Calendar.ZONE_OFFSET);// 时间偏移量
		int dstOffset = calendar.get(Calendar.DST_OFFSET);// 夏令时差
		calendar.add(Calendar.MILLISECOND, -(zoneOffset + dstOffset));// UTC时间算法
		return formatDateTimeStr(calendar.getTime(), format);
	}

	/**
	 * <pre>
	 * 描述:格式化合并日期时间(格式:yyyy-MM-dd ~ MM-dd 或 yyyy-MM-dd HH:mm ~ yyyy-MM-dd HH:mm)
	 * 作者:ZhangYi
	 * 时间:2016.10.24
	 * 参数:参数列表
	 * @param start 		开始时间
	 * @param end 		结束时间
	 * @param showTime 	显示方式(false:仅显示日期[格式:yyyy-MM-dd],true:显示时间[格式:yyyy-MM-dd HH:mm])
	 * </pre>
	 */
	public static String formatRangeDateTime(Date start, Date end, boolean showTime) {
		String startTime = formatDateTimeStr(start, DEFAULT_FORMAT_DATE);
		String endTime = formatDateTimeStr(end, DEFAULT_FORMAT_DATE);
		if (!showTime) {
			if (formatYear(start) == formatYear(end)) {
				if (startTime == endTime) {
					return startTime;
				}
				return startTime + " ~ " + formatDateTimeStr(end, FORMAT_PATTERN_DATE);
			}
			return startTime + " ~ " + endTime;
		} else {
			if (startTime == endTime) {
				return formatDateTimeStr(start, FORMAT_DATE_TIME) + " ~ " + formatDateTimeStr(end, FORMAT_PATTERN_TIME);
			}
			if (formatYear(start) == formatYear(end)) {
				return formatDateTimeStr(start, FORMAT_DATE_TIME) + " ~ " + formatDateTimeStr(end, FORMAT_PATTERN_DATE_TIME);
			}
			return formatDateTimeStr(start, FORMAT_DATE_TIME) + " ~ " + formatDateTimeStr(end, FORMAT_DATE_TIME);
		}
	}

	public static String formatRangeDateStr(Date start, Date end) {
		String rangeTime = formatDateTimeStr(start, "MM月dd日");
		rangeTime += " " + formatDateTimeStr(start, "HH:mm");
		rangeTime += "~" + formatDateTimeStr(end, "HH:mm");
		return rangeTime;
	}

	public static String formatRangeDateStrEng(Date start, Date end) {
		String rangeTime = formatDateTimeStr(start, "yyyy-MM-dd");
		rangeTime += " " + formatDateTimeStr(start, "HH:mm");
		rangeTime += "~" + formatDateTimeStr(end, "HH:mm");
		return rangeTime;
	}

	public static void main(String[] args) {
		// String date = "1970-01-01 00:00:00";
		// String time = "1461032462000";
		// String to = "1462032000000";
		// long t1 = Long.valueOf(time) / (24 * 60 * 60 * 1000);
		// System.out.println(t1);
		// System.out.println(DateUtil.getDate(date).getTime());
		// System.out.println(DateUtil.formatDateTime(time));
		// System.out.println(DateUtil.getDate("2016-05-01"));
		// System.out.println(DateUtil.getChinaDateYMD(new Date()));
		// System.out.println(DateUtil.formatDateTimeStr(getLastTime(new Date())));
		// Calendar calender1 = Calendar.getInstance();
		// calender1.setTime(new Date());
		// calender1.add(Calendar.DATE, 30);
		// String dateTime1 = formatDateTimeStr(calender1.getTime());
		// System.out.println(dateTime1);
		// GregorianCalendar calendar = new GregorianCalendar();
		// calendar.setTime(new Date());
		// calendar.add(Calendar.MONTH, 2);
		// calendar.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
		// calendar.set(Calendar.DAY_OF_WEEK_IN_MONTH, 4);
		String CHINA_FORMAT_DATE_TIME =
				"G yyyy年MM月dd日 a HH时mm分ss秒SSS毫秒 zZ(本年第w周,本月第W周,本年第D天,本月第F星期的E)";
		String dateTime =
				formatDateTimeStr(formatDateTime("2016-07-31 13:26:50"), CHINA_FORMAT_DATE_TIME);
		System.out.println(dateTime);
		Date today = new Date();
		System.out.println(formatFirstTime(today));
		System.out.println(formatLastTime(today));
		System.out.println(formatWeekFirstTime(today));
		System.out.println(formatWeekLastTime(today));
		System.out.println(formatMonthFirstTime(today));
		System.out.println(formatMonthLastTime(today));

		Date start = formatDate("2016-11-01");
		Date end = formatDate("2016-11-23");
		System.out.println(intervalMonths(start, end));
	}
}

