using Microsoft.Extensions.Logging;
using SimpleETL.DB.Common;
using SimpleETL.DB.Trans.Interface;

namespace SimpleETL.DB.Trans
{
    public class TransferFactory : ITransferFactory
    {

        private readonly ILoggerFactory _loggerFactory;

        public TransferFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public IDBTransfer GetDBTransfer(DatabaseType sourceType ,string sourceStr,DatabaseType targetType,string targetStr)
        {
            switch (sourceType)
            {
                case DatabaseType.SqlServer:
                    return new SQLTransferImpl(
                        logger: _loggerFactory.CreateLogger<SQLTransferImpl>(),
                        sqlServerConnectionString: sourceStr,
                        targetDB: targetType,
                        targetDBConnectionString: targetStr);
                    break;
                case DatabaseType.MySQL:
                case DatabaseType.Oracle:
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
