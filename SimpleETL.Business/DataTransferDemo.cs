using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SimpleETL.DB.Common;
using SimpleETL.DB.Trans.Interface;

namespace SimpleETL.Business
{
    /// <summary>
    /// sqlserver trans to mysql
    /// </summary>
    public class DataTransferDemo
    {
        public DataTransferDemo(ILogger logger, ITransferFactory transferFactory, IConfiguration configure, string sourcetype, string targettype)
        {
            _logger = logger;
            _transferFactory = transferFactory;
            _configure = configure;

            Enum.TryParse<DatabaseType>(sourcetype, true, out DatabaseType sourcetypeEnum);
            _transferFactory.SourceType = sourcetypeEnum;
            _transferFactory.SourceConStr = _configure?[sourcetype];

            Enum.TryParse<DatabaseType>(targettype, true, out DatabaseType targettypeEnum);
            _transferFactory.TargetType = targettypeEnum;
            _transferFactory.TargetConStr = _configure?[targettype];

            _dBTrans = _transferFactory.GetDBTransfer();
            _Helper = new DBHelperFactory(_logger, _transferFactory.SourceType, _transferFactory.SourceConStr).BuilderDBHelper();
        }
        private readonly ILogger _logger;
        private readonly ITransferFactory _transferFactory;
        private readonly IConfiguration _configure;
        private readonly IDBTransfer _dBTrans;
        private const string CompareTablePrefix = "bigdata_";
        private readonly IDBHelper _Helper;

        public async Task<long> TransTestTableData(string sourcetype, string sourceStr, string targettype, string targetStr)
        {
            var tablename = "BULKCOPYDEMO";
            var sSql = "select NAME,CONTEXT,CREATETIME,CREATETICKS from BULKCOPYDEMO";
            if (Enum.TryParse<DatabaseType>(sourcetype, true, out DatabaseType stype) &&
                Enum.TryParse<DatabaseType>(targettype, true, out DatabaseType ttype))
            {
                var result = await _transferFactory
                    .GetDBTransfer()
                    .BulkCopy(sSql, tablename, null);
                _logger.LogInformation($"【{sourcetype} To {targettype}】{tablename} BulkCopy Nums:{result}");
                return result;
            }
            return 0;
        }
    }
}
