using Microsoft.Extensions.Configuration;

namespace SimpleETL.DB.Encrypt
{
    public class CustomConfigProvider : ConfigurationProvider, IConfigurationSource
    {
        public CustomConfigProvider()
        {
        }

        public override void Load()
        {
            Data = UnencryptMyConfiguration();
        }

        private IDictionary<string, string> UnencryptMyConfiguration()
        {
            // do whatever you need to do here, for example load the file and unencrypt key by key
            //Like:
            //
            var configValues = new Dictionary<string, string>
            {
                {"oracle", Environment.GetEnvironmentVariable("oracle")},
                {"sqlserver", Environment.GetEnvironmentVariable("sqlserver")}
            };


            return configValues;
        }

        private IDictionary<string, string> CreateAndSaveDefaultValues(IDictionary<string, string> defaultDictionary)
        {
            var configValues = new Dictionary<string, string>
            {
                //{"oracle", "SERVER=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=DEMO)));uid=test;pwd=test;"},
                //{"sqlserver", "Server=127.0.0.1;Database=test;User Id=sa;Password=123456;TrustServerCertificate=true"}
            };
            return configValues;
        }

        public IConfigurationProvider Build(IConfigurationBuilder builder)
        {
            return new CustomConfigProvider();
        }
    }
}