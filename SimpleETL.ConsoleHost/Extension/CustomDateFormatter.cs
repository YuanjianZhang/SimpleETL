using System.Globalization;

namespace SimpleETL.ConsoleHost.Extension
{
    public class CustomDateFormatter : IFormatProvider
    {
        readonly IFormatProvider basedOn;
        readonly string shortDatePattern;
        /// <summary>
        /// 自定义日期格式化，默认区域为国内
        /// </summary>
        public CustomDateFormatter()
        {
            shortDatePattern = "yyyy-MM-dd HH:mm:ss.fff";
            basedOn = new CultureInfo("zh-CN");
        }
        public CustomDateFormatter(string shortDatePattern, IFormatProvider basedOn)
        {
            this.shortDatePattern = shortDatePattern;
            this.basedOn = basedOn;
        }

        public object GetFormat(Type formatType)
        {
            if (formatType == typeof(DateTimeFormatInfo))
            {
                var basedOnFormatInfo = (DateTimeFormatInfo)basedOn.GetFormat(formatType);
                var dateFormatInfo = (DateTimeFormatInfo)basedOnFormatInfo.Clone();
                dateFormatInfo.ShortDatePattern = shortDatePattern;
                return dateFormatInfo;
            }
            return basedOn.GetFormat(formatType);
        }
    }
}
