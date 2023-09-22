using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using SimpleETL.DB.Common.MySQL;
using SimpleETL.DB.Common.SQL;

namespace SimpleETL.DB.Common
{
    public class DBHelperFactory
    {
        public DBHelperFactory(ILogger logger, DatabaseType databaseType, string connectionString)
        {
            _logger = logger;
            _databaseType = databaseType;
            _connectionString = connectionString;
        }
        private DatabaseType _databaseType;
        private readonly ILogger _logger;
        private readonly string _connectionString;

        public IDBHelper BuilderDBHelper()
        {
            switch (_databaseType)
            {
                case DatabaseType.SqlServer:
                    return new SqlHelper(_logger, _connectionString);
                    break;
                case DatabaseType.MySQL:
                    return new MySqlHelper(_logger, _connectionString);
                    break;
                case DatabaseType.Oracle:
                    return new OracleHelper(_logger, _connectionString);
                    break;
                default:
                    throw new NotImplementedException("数据库类型目前不支持");
            }
        }
    }
}
