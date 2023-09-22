using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SimpleETL.DB.Common;
using SimpleETL.DB.Common.SQL;
using SimpleETL.DB.Trans;
using SimpleETL.DB.Trans.Interface;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.Business
{
    /// <summary>
    /// sqlserver trans to mysql
    /// </summary>
    public class DataTransferDemo
    {
        public DataTransferDemo(ILogger<DataTransferDemo> logger, ITransferFactory transferFactory)
        {
            _transferFactory = transferFactory;
        }
        private readonly ILogger<DataTransferDemo> _logger;
        private readonly ITransferFactory _transferFactory;

        public async Task<long> TransTestTableData(string sourcetype, string sourceStr, string targettype, string targetStr)
        {
            var tablename = "BULKCOPYDEMO";
            var sSql = "select NAME,CONTEXT,CREATETIME,CREATETICKS from BULKCOPYDEMO";
            if (Enum.TryParse<DatabaseType>(sourcetype, true, out DatabaseType stype) &&
                Enum.TryParse<DatabaseType>(targettype, true, out DatabaseType ttype))
            {
                var result = await _transferFactory
                    .GetDBTransfer(sourceType: stype,
                                   sourceStr: sourceStr,
                                   targetType: ttype,
                                   targetStr: targetStr)
                    .BulkCopy(sSql, tablename, null);
                _logger.LogInformation($"【{sourcetype} To {targettype}】{tablename} BulkCopy Nums:{result}");
                return result;
            }
            return 0;
        }
    }
}
