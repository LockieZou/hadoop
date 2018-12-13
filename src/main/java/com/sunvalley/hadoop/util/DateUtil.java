package com.sunvalley.hadoop.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import javax.xml.datatype.XMLGregorianCalendar;


/**
 * 日期工具类
 */
public class DateUtil {
    public static final String TIME_PATTERN = "HH:mm";
    public static final String DATE_FORMAT_STRING1 = "yyyy-MM-dd";
    public static final String DATE_FORMAT_STRING2 = "yyyy/MM/dd";
    public static final String DATE_FORMAT_STRING3 = "yyyy年MM月dd日";
    public static final String DATE_FORMAT_STRING4 = "yyyyMMdd";
    public static final String DATE_FORMAT_STRING5 = "MMM-dd-yy";
    public static final String DATE_FORMAT_STRING6 = "dd.MM.yyyy";
    public static final String DATE_FORMAT_STRING7 = "yyyyMM";
    public static final String DATE_FORMAT_STRING8 = "dd-MMM-yy";
    public static final String DATE_FORMAT_STRING9 = "yyMMdd";
    public static final String en_FORMAT = "MMM dd yyyy";
    public static final String PAGE_SHOW_FORMAT_STRING = "MMM dd, yyyy";
    public static final String DATE_TIME_FORMAT_STRING1 = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING14 = "yyyy/MM/dd HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING2 = " HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING3 = "yyyy年MM月dd日 HH时mm分ss秒";
    public static final String DATE_TIME_FORMAT_STRING4 = "yyyyMMddHHmmss";
    public static final String DATE_TIME_FORMAT_STRING5 = "dd.MM.yyyy HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING52 = "dd.MM.yyyy";
    public static final String DATE_TIME_FORMAT_STRING6 = "MM/dd/yyyy HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING7 = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING8 = "MM/dd/yyyy HH:mm a";
    public static final String DATE_TIME_FORMAT_STRING9 = "yyyyMMddHHmm";
    public static final String DATE_TIME_FORMAT_STRING10 = "MM/dd/yyyy HH:mm:ss a";
    public static final String DATE_TIME_FORMAT_STRING11 = "yyyy-MM-dd-HH-mm-ss-ssss";
    public static final String DATE_TIME_FORMAT_STRING12 = "MM/dd/yyyy";
    public static final String DATE_TIME_FORMAT_STRING13 = "dd/MM/yyyy HH:mm:ss";
    public static final String DATE_TIME_FORMAT_STRING15 = "MM/dd/yyyy HH:mm";
    public static final String YEAR_FORMAT_STRING = "yyyy";
    public static final String TIME_FORMAT_STRING1 = "HH:mm:ss";
    public static final String TIME_FORMAT_STRING2 = "HH时mm分ss秒";
    public static final String UTC_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'+'00:00";
    public static final String DATE_TIME_FORMAT_ISO = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String SHORT_DATE_PATTERN_LINE = "yyyy-MM-dd";

    public static Date getCurrentDateTime() {
        Date date = new Date(System.currentTimeMillis());
        return date;
    }

    public static Date getCurrentDate() {
        Date date = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
        return date;
    }

    public static Date getDefUTCTime() {
        return new Date(0L);
    }

    public static Date parseISODate(String strDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            return format.parse(strDate);
        } catch (Exception var2) {
            var2.printStackTrace();
            return null;
        }
    }

    public static long daysDiff(Date startDate, Date endDate) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate localDateStart = startDate.toInstant().atZone(zoneId).toLocalDate();
        LocalDate localDateEnd = endDate.toInstant().atZone(zoneId).toLocalDate();
        return ChronoUnit.DAYS.between(localDateStart, localDateEnd);
    }

    public static String getYearString(String yearFormat, Date date) {
        SimpleDateFormat format = new SimpleDateFormat(yearFormat);
        return format.format(date);
    }

    public static String getDateString(String dateFormat, Date date) {
        if (date == null) {
            return "";
        } else {
            SimpleDateFormat format = new SimpleDateFormat(dateFormat);
            return format.format(date);
        }
    }

    public static String getDateString2(String dateFormat, Date date) {
        SimpleDateFormat format = new SimpleDateFormat(dateFormat, Locale.ENGLISH);
        return format.format(date);
    }

    public static String getDateTimeString(String dateTimeFormat, Date date) {
        SimpleDateFormat format = new SimpleDateFormat(dateTimeFormat);
        return format.format(date);
    }

    public static String getTimeString(String timeFormat, Date date) {
        SimpleDateFormat format = new SimpleDateFormat(timeFormat);
        return format.format(date);
    }

    public static Date parseDate(String dateFormat, String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        return format.parse(date);
    }

    public static Date parseDate2(String dateFormat, String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(dateFormat, Locale.ENGLISH);
        return format.parse(date);
    }

    public static Date parseDate3(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        try {
            return format.parse(format.format(date));
        } catch (ParseException var3) {
            throw var3;
        }
    }

    public static Date parseDate4(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        try {
            return format.parse(format.format(date));
        } catch (ParseException var3) {
            throw var3;
        }
    }

    public static Date parseDate14(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try {
            return format.parse(format.format(date));
        } catch (ParseException var3) {
            throw var3;
        }
    }

    public static Date parseDateTime(String dateTimeFormat, String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(dateTimeFormat);
        return format.parse(date);
    }

    public static Date parseTime(String timeFormat, String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(timeFormat);
        return format.parse(date);
    }

    public static String getDateString1(Date date) {
        return getDateString("yyyy-MM-dd", date);
    }

    public static String getDateString2(Date date) {
        return getDateString("yyyy/MM/dd", date);
    }

    public static String getDateStringCN(Date date) {
        return getDateString("yyyy年MM月dd日", date);
    }

    public static String getDateStringCompact(Date date) {
        return getDateString("yyyyMMdd", date);
    }

    public static String getDateTimeString4(Date date) {
        return getDateString("yyyyMMddHHmmss", date);
    }

    public static String getDateString5(Date date) {
        return getDateString2("MMM-dd-yy", date);
    }

    public static String getDateTimeString1(Date date) {
        return getDateTimeString("yyyy-MM-dd HH:mm:ss", date);
    }

    public static String getDateTimeString2(Date date) {
        return getDateTimeString(" HH:mm:ss", date);
    }

    public static String getDateTimeString13(Date date) {
        return getDateTimeString("dd/MM/yyyy HH:mm:ss", date);
    }

    public static String getDateTimeStringCN(Date date) {
        return getDateTimeString("yyyy年MM月dd日 HH时mm分ss秒", date);
    }

    public static String getTimeString(Date date) {
        return getTimeString("HH:mm:ss", date);
    }

    public static String getTimeStringCN(Date date) {
        return getTimeString("HH时mm分ss秒", date);
    }

    public static String getYearString(Date date) {
        return getYearString("yyyy", date);
    }

    public static Date getParseDate1(String date) throws ParseException {
        return parseDate("yyyy-MM-dd", date);
    }

    public static Date getParseDate14(String date) throws ParseException {
        return parseDate("yyyy/MM/dd HH:mm:ss", date);
    }

    public static Date getParseDates(String date) throws ParseException {
        return parseDate("MMM dd yyyy", date);
    }

    public static Date getParseDate3(String date) throws ParseException {
        return parseDate("yyyy-MM-dd HH:mm:ss", date);
    }

    public static Date getParseDate4(String date) throws ParseException {
        return parseDate("yyyyMMddHHmmss", date);
    }

    public static Date getParseDate2(String date) throws ParseException {
        return parseDate("yyyy/MM/dd", date);
    }

    public static int getYearOfDate(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String d = format.format(date);
        return Integer.parseInt(d.substring(0, 4));
    }

    public static int getMonthOfDate(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String d = format.format(date);
        return Integer.parseInt(d.substring(5, 7));
    }

    public static int getDayOfDate(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String d = format.format(date);
        return Integer.parseInt(d.substring(8, 10));
    }

    public static Date getFirstDay(String year, String month) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.parse(year + "-" + month + "-1");
    }

    public static Date getFirstDay(int year, int month) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.parse(year + "-" + month + "-1");
    }

    public static Date getLastDay(String year, String month) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = format.parse(year + "-" + month + "-1");
        Calendar scalendar = new GregorianCalendar();
        scalendar.setTime(date);
        scalendar.add(2, 1);
        scalendar.add(5, -1);
        date = scalendar.getTime();
        return date;
    }

    public static Date getLastDay(int year, int month) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = format.parse(year + "-" + month + "-1");
        Calendar scalendar = new GregorianCalendar();
        scalendar.setTime(date);
        scalendar.add(2, 1);
        scalendar.add(5, -1);
        date = scalendar.getTime();
        return date;
    }

    public static long getDistinctMonth(String fromDate, String toDate) throws ParseException {
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");
        long monthCount = 0L;
        Date d1 = d.parse(fromDate);
        Date d2 = d.parse(toDate);
        monthCount = (long)((d2.getYear() - d1.getYear()) * 12 + d2.getMonth() - d1.getMonth());
        return monthCount;
    }

    public static long getDistinctDay(String fromDate, String toDate) throws ParseException {
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");
        long dayCount = 0L;
        Date d1 = d.parse(fromDate);
        Date d2 = d.parse(toDate);
        dayCount = (d2.getTime() - d1.getTime()) / 86400000L;
        return dayCount;
    }

    public static long getDistinctDay(Date fromDate, Date toDate) throws ParseException {
        long dayCount = 0L;
        dayCount = (toDate.getTime() - fromDate.getTime()) / 86400000L;
        return dayCount;
    }

    public DateUtil() {
    }

    public static Date convertStringToDate(String aMask, String strDate) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat(aMask);

        try {
            Date date = df.parse(strDate);
            return date;
        } catch (ParseException var5) {
            throw new ParseException(var5.getMessage(), var5.getErrorOffset());
        }
    }

    public static String getTimeNow(Date theTime) {
        return getDateTime("HH:mm", theTime);
    }

    public static String getDateTime(String aMask, Date aDate) {
        SimpleDateFormat df = null;
        String returnValue = "";
        if (aDate == null) {
            return "";
        } else {
            df = new SimpleDateFormat(aMask);
            returnValue = df.format(aDate);
            return returnValue;
        }
    }

    public static boolean isDate(String strFormat, String strDate) {
        SimpleDateFormat df = new SimpleDateFormat(strFormat, Locale.ENGLISH);

        try {
            df.parse(strDate);
            return true;
        } catch (ParseException var4) {
            return false;
        }
    }

    public static boolean isDate(String strDate) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        try {
            df.parse(strDate);
            return true;
        } catch (ParseException var3) {
            return false;
        }
    }

    public static String convertDate(String aMask, String strDate) {
        SimpleDateFormat df = new SimpleDateFormat(aMask, Locale.ENGLISH);
        String str = "";
        str = df.format(new Date(strDate));
        return str;
    }

    public static String adjustDate(Date oldDate, int days) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(oldDate);
        calendar.add(5, days);
        Date newDate = calendar.getTime();
        return sf.format(newDate);
    }

    public static Date adjustDateForReturnDate(Date oldDate, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(oldDate);
        calendar.add(5, days);
        Date newDate = calendar.getTime();
        return newDate;
    }

    public static String adjustDateForDateTime(Date oldDate, int days) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(oldDate);
        calendar.add(5, days);
        Date newDate = calendar.getTime();
        return sf.format(newDate);
    }

    public static String formatDateTime(Object o, String format) {
        String s = String.valueOf(o);
        if (s != null && !"".equals("")) {
            Date dt = (Date)o;
            String ds = getDateTimeString(format, dt);
            return ds;
        } else {
            return "";
        }
    }

    public static int spaceDays(Date beforeDate, Date afterDate) {
        long space = afterDate.getTime() - beforeDate.getTime();
        return (int)(space / 86400000L);
    }

    public static boolean compareDates(Date firstDate, Date secondDate) {
        double first = Math.floor((double)(firstDate.getTime() / 86400000L));
        double after = Math.floor((double)(secondDate.getTime() / 86400000L));
        return first > after;
    }

    public static String formatDate(Date date) {
        SimpleDateFormat f = new SimpleDateFormat("MMM dd yyyy", Locale.US);
        return f.format(date);
    }

    public static String getFormatDate(String format, String datetime) {
        SimpleDateFormat df = new SimpleDateFormat(format);
        String strDate = "";

        try {
            Date date = df.parse(datetime);
            df.applyPattern(format);
            strDate = df.format(date);
        } catch (ParseException var6) {
            ;
        }

        return strDate;
    }

    public static String formatDates(Date date) {
        SimpleDateFormat f = new SimpleDateFormat("MMM dd yyyy", Locale.US);
        return f.format(date);
    }

    public static Date getInitDate() throws Exception {
        return parseDate("yyyy-MM-dd", "0000-00-00");
    }

    public static boolean isBigDate(String strFormat, String strDate) {
        SimpleDateFormat df = new SimpleDateFormat(strFormat, Locale.ENGLISH);

        try {
            Date date = df.parse(strDate);
            DateFormat df2 = new SimpleDateFormat("yy-MM-dd");
            return date.compareTo(df2.parse("1900-01-01")) >= 0;
        } catch (ParseException var5) {
            return false;
        }
    }

    public static Date getYesterday() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(5, -1);
        return cal.getTime();
    }

    public static Date getDateByWeek(int week) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(4, week);
        return cal.getTime();
    }

    public static Date getDateBydays(Date date, int days) {
        Calendar scalendar = Calendar.getInstance();
        scalendar.setTime(date);
        scalendar.add(5, days);
        return scalendar.getTime();
    }

    public static int nDaysBetweenTwoDate(Date firstDate, Date secondDate) {
        int nDay = (int)(secondDate.getTime() - firstDate.getTime());
        return nDay;
    }

    public static int nDaysBetweenTwoDate_min(Date firstDate, Date secondDate) {
        int nDay = (int)((secondDate.getTime() - firstDate.getTime()) / 60000L);
        return nDay;
    }

    public static Date string2Timezone(Date srcDateTime, String dstTimeZoneId) {
        int diffTime = getDiffTimeZoneRawOffset(dstTimeZoneId);
        long nowTime = srcDateTime.getTime();
        long newNowTime = nowTime - (long)diffTime;
        srcDateTime = new Date(newNowTime);
        return srcDateTime;
    }

    private static int getDiffTimeZoneRawOffset(String timeZoneId) {
        return TimeZone.getDefault().getRawOffset() - TimeZone.getTimeZone(timeZoneId).getRawOffset();
    }

    public static int nDaysBetweenTwoDate_po(Date firstDate, Date secondDate) {
        int nDay = (int)((secondDate.getTime() - firstDate.getTime()) / 1000L);
        return nDay;
    }

    public static boolean compareDatesEq(Date firstDate, Date secondDate) {
        double first = Math.floor((double)(firstDate.getTime() / 86400000L));
        double after = Math.floor((double)(secondDate.getTime() / 86400000L));
        return first >= after;
    }

    public static int nDaysBetweenTwoDate_day(Date firstDate, Date secondDate) {
        return (int)((secondDate.getTime() - firstDate.getTime()) / 86400000L);
    }

    public static Date parseXMLUTCDate(String dateStr) throws ParseException {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+'00:00", Locale.ENGLISH);
        return sf.parse(dateStr);
    }

    public static Date parsePageShowDate(String dateStr) throws ParseException {
        SimpleDateFormat sf = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH);
        return sf.parse(dateStr);
    }

    public static Date trunsXMLGregorianCalendar(XMLGregorianCalendar xmlGregorianCalendar) {
        if (xmlGregorianCalendar == null) {
            return null;
        } else {
            Calendar cal = Calendar.getInstance();
            cal.set(xmlGregorianCalendar.getYear(), xmlGregorianCalendar.getMonth() - 1, xmlGregorianCalendar.getDay(), xmlGregorianCalendar.getHour(), xmlGregorianCalendar.getMinute(), xmlGregorianCalendar.getSecond());
            return cal.getTime();
        }
    }

    public static String getUtc2Local(String utcTime, String utcTimePatten, String localTimePatten) {
        SimpleDateFormat utcFormater = new SimpleDateFormat(utcTimePatten);
        utcFormater.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date gpsUTCDate = null;

        try {
            gpsUTCDate = utcFormater.parse(utcTime);
        } catch (ParseException var7) {
            var7.printStackTrace();
            return utcTime;
        }

        SimpleDateFormat localFormater = new SimpleDateFormat(localTimePatten);
        localFormater.setTimeZone(TimeZone.getDefault());
        String localTime = localFormater.format(gpsUTCDate.getTime());
        return localTime;
    }

    public static String getBeijingDate(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date returnDate = format.parse(date);
        long Time = returnDate.getTime() / 1000L - 57600L;
        returnDate.setTime(Time * 1000L);
        return format.format(returnDate);
    }

    public static void main(String[] args) throws ParseException {
        Date cusDate = parseDate("yyyy-MM-dd HH:mm:ss", "2018-03-04 12:22:30");
        LocalDateTime localDateTime = LocalDateTime.now();
        ZoneId zone = ZoneId.of("America/Los_Angeles");
        Instant instant = localDateTime.atZone(zone).toInstant();
        Date date = Date.from(instant);
        Date d1 = parseDate("yyyy-MM-dd HH:mm:ss", "2018-03-21 17:00:02");
        Date d2 = parseDate("yyyy-MM-dd HH:mm:ss", "2018-03-20 00:00:00");
        daysDiff(d1, d2);
    }
}


