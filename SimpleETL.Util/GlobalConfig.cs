using Microsoft.Extensions.Configuration;

namespace SimpleETL.Util
{
    public class GlobalConfig
    {
        public static IConfigurationManager Configure = null;
        private static string ConvertToJsonPath(string key) => key.Replace('_', ':');
        public static bool DBContext_EnableLog => Configure.GetValue<bool>(ConvertToJsonPath(nameof(DBContext_EnableLog)));
        public static bool DBContext_EnableDetailedErrors => Configure.GetValue<bool>(ConvertToJsonPath(nameof(DBContext_EnableDetailedErrors)));
        public static bool DBContext_EnableSensitiveDataLog => Configure.GetValue<bool>(ConvertToJsonPath(nameof(DBContext_EnableSensitiveDataLog)));
    }
}
