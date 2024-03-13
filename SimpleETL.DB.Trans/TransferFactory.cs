using Microsoft.Extensions.Logging;
using SimpleETL.DB.Common;
using SimpleETL.DB.Trans.Interface;

namespace SimpleETL.DB.Trans
{
    public class TransferFactory : ITransferFactory
    {
        public TransferFactory(ILogger logger)
        {
            this.logger = logger;
        }
        private readonly ILogger logger;
        private DatabaseType _sourceDBType;
        private DatabaseType _targetDBType;
        private string _sourceConnectionString;
        private string _targetConnectionString;

        public DatabaseType SourceType { get => _sourceDBType; set => _sourceDBType = value; }
        public DatabaseType TargetType { get => _targetDBType; set => _targetDBType = value; }
        public string SourceConStr { get => _sourceConnectionString; set => _sourceConnectionString = value; }
        public string TargetConStr { get => _targetConnectionString; set => _targetConnectionString = value; }

        public IDBTransfer GetDBTransfer()
        {
            switch (SourceType)
            {
                case DatabaseType.SqlServer:
                    return new SQLTransferImpl(
                        logger: logger,
                        sqlServerConnectionString: this.SourceConStr,
                        targetDB: this.TargetType,
                        targetDBConnectionString: this.TargetConStr);
                case DatabaseType.MySQL:
                case DatabaseType.Oracle:
                default:
                    throw new NotImplementedException();
            }
        }

    }
}
