using Microsoft.Extensions.Configuration;

namespace SimpleETL.Encrypt
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
            var configValues = new Dictionary<string, string>
            {
                //{"SourceSQLServer", "Data Source=127.0.0.1,1433;uid=sa;pwd=admin;database=test;TrustServerCertificate=true"},
                //{"targetMySQL", "Server=192.168.10.5;Port=3306;Database=testdb;Uid=testdb;Pwd=123456789;AllowLoadLocalInfile=true;"}
            };


            return configValues;
        }

        private IDictionary<string, string> CreateAndSaveDefaultValues(IDictionary<string, string> defaultDictionary)
        {
            var configValues = new Dictionary<string, string>
            {
                //{"SourceSQLServer", "Data Source=127.0.0.1,1433;uid=sa;pwd=admin;database=test;TrustServerCertificate=true"},
                //{"targetMySQL", "Server=192.168.10.5;Port=3306;Database=testdb;Uid=testdb;Pwd=123456789;AllowLoadLocalInfile=true;"}
            };
            return configValues;
        }

        public IConfigurationProvider Build(IConfigurationBuilder builder)
        {
            return new CustomConfigProvider();
        }
    }
}