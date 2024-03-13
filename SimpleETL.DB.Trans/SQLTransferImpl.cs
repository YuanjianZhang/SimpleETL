using Microsoft.Extensions.Logging;
using MySqlConnector;
using Oracle.ManagedDataAccess.Client;
using SimpleETL.DB.Common;
using SimpleETL.DB.Common.MySQL;
using SimpleETL.DB.Common.SQL;
using SimpleETL.DB.Trans.Interface;
using System.Data;
using System.Data.SqlClient;

namespace SimpleETL.DB.Trans
{
    /// <summary>
    /// SQL Server 数据传输实现类【源数据库：SQLServer】
    /// </summary>
    public class SQLTransferImpl : IDBTransfer
    {
        #region 构造函数
        public SQLTransferImpl(ILogger logger, string sqlServerConnectionString, DatabaseType targetDB, string targetDBConnectionString)
        {
            _logger = logger;
            _sqlHelper = new SqlHelper(_logger, sqlServerConnectionString);
            _targetDB = targetDB;
            _sourceConnectionString = sqlServerConnectionString;
            _targetConnectionString = targetDBConnectionString;
            switch (_targetDB)
            {
                case DatabaseType.SqlServer:
                    _targetSqlHelper = new SqlHelper(_logger, _targetConnectionString);
                    break;
                case DatabaseType.MySQL:
                    _mySqlHelper = new Common.MySQL.MySqlHelper(_logger, _targetConnectionString);
                    break;
                case DatabaseType.Oracle:
                    _oracleHelper = new OracleHelper(_logger, _targetConnectionString);
                    break;
                default:
                    throw new NotImplementedException();
                    break;
            }
        }

        #endregion

        #region 属性
        private ILogger _logger;
        private DatabaseType _targetDB;
        private string _sourceConnectionString;
        private readonly SqlHelper _sqlHelper;

        private string _targetConnectionString;
        private readonly SqlHelper _targetSqlHelper;
        private readonly Common.MySQL.MySqlHelper _mySqlHelper;
        private readonly OracleHelper _oracleHelper;

        public DatabaseType SouceDBType => DatabaseType.SqlServer;
        public DatabaseType TargetDBType { get => _targetDB; set => _targetDB = value; }

        #endregion

        #region 接口实现
        public async Task<long> BulkCopy(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter)
        {
            try
            {
                switch (TargetDBType)
                {
                    case DatabaseType.MySQL:
                        return await BulkCopyToMySQL(sourceSql, targetTableName, sourceParameter);
                        break;
                    case DatabaseType.SqlServer: throw new NotImplementedException();
                    case DatabaseType.Oracle:
                        return await BulkCopyToOracle(sourceSql, targetTableName, sourceParameter);
                        break;
                    default: throw new NotImplementedException();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public async Task<int> BulkCopyTrans(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter)
        {
            try
            {
                switch (TargetDBType)
                {
                    case DatabaseType.MySQL:
                        return await BulkCopyToMySQLTrans(sourceSql, targetTableName, sourceParameter);
                        break;
                    case DatabaseType.SqlServer:
                    case DatabaseType.Oracle:
                    default: throw new NotImplementedException();
                }
            }
            catch
            {
                throw;
            }
        }
        #endregion

        #region protected Method

        /// <summary>
        /// 批量复制到MySQL
        /// </summary>
        /// <param name="sourceSql"></param>
        /// <param name="targetTableName"></param>
        /// <param name="sourceParameter"></param>
        /// <returns></returns>
        protected async Task<int> BulkCopyToMySQL(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter)
        {
            try
            {

                var mapping = _mySqlHelper.GreanMappingToMySQL(
                   await _mySqlHelper.GetTableSchemaDict(targetTableName),
                   await _sqlHelper.GetResultTableSchema_DictAsync(sourceSql, sourceParameter));
                if (mapping == null || mapping.Count <= 0) throw new Exception("批量复制失败，无匹配列！");


                using var targetConnection = new MySqlConnection(_targetConnectionString);
                using var sourceConnection = new SqlConnection(_sourceConnectionString);
                using var cmd = _sqlHelper.createSqlCommand(sourceSql, sourceConnection, sourceParameter);
                using var reader = await cmd.ExecuteReaderAsync();
                var bulkCopy = new MySqlBulkCopy(targetConnection);
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = targetTableName;
                bulkCopy.ColumnMappings.AddRange(mapping);
                var result = await bulkCopy.WriteToServerAsync(reader);
                if (result.Warnings.Count > 0)
                {
                    _logger.LogWarning($"BulkCopyToMySQL Warnings Nums:{result.Warnings.Count}");
                    foreach (var item in result.Warnings)
                    {
                        _logger.LogWarning($"{item.Code}：{item.Message}");
                    }
                }
                cmd.Parameters.Clear();
                return result.RowsInserted;
            }
            catch
            {
                throw;
            }
        }
        /// <summary>
        /// 批量复制到MySQL【事务】
        /// </summary>
        /// <param name="sourceSql"></param>
        /// <param name="targetTableName"></param>
        /// <param name="sourceParameter"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        protected async Task<int> BulkCopyToMySQLTrans(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter)
        {

            try
            {
                var mapping = _mySqlHelper.GreanMappingToMySQL(
                   await _mySqlHelper.GetTableSchemaDict(targetTableName),
                   await _sqlHelper.GetResultTableSchema_DictAsync(sourceSql, sourceParameter));
                if (mapping == null || mapping.Count <= 0) throw new Exception("批量复制失败，无匹配列！");

                using var targetConnection = new MySqlConnection(_targetConnectionString);
                using var trans = await targetConnection.BeginTransactionAsync();
                using var sourceConnection = new SqlConnection(_sourceConnectionString);
                using var cmd = _sqlHelper.createSqlCommand(sourceSql, sourceConnection, sourceParameter);
                using var reader = await cmd.ExecuteReaderAsync();
                try
                {
                    var bulkCopy = new MySqlBulkCopy(targetConnection, trans);
                    bulkCopy.DestinationTableName = targetTableName;
                    bulkCopy.ColumnMappings.AddRange(mapping);
                    var result = await bulkCopy.WriteToServerAsync(reader);
                    await trans.CommitAsync();
                    if (result.Warnings.Count > 0)
                    {
                        _logger.LogWarning($"BulkCopyToMySQLTrans Warnings Nums:{result.Warnings.Count}");
                        foreach (var item in result.Warnings)
                        {
                            _logger.LogWarning($"{item.Code}：{item.Message}");
                        }
                    }

                    cmd.Parameters.Clear();
                    return result.RowsInserted;
                }
                catch
                {
                    await trans.RollbackAsync();
                    throw;
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// 批量复制到Oracle【日期类型数据需要转储，使用varchar字段保存，跑sql脚本转换。】
        /// </summary>
        /// <param name="sourceSql"></param>
        /// <param name="targetTableName"></param>
        /// <param name="sourceParameter"></param>
        /// <returns></returns>
        protected async Task<long> BulkCopyToOracle(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter)
        {
            try
            {
                var mapping = _oracleHelper.GreanMappingToOracle(
                   await _oracleHelper.GetTableSchemaDict(targetTableName),
                   await _sqlHelper.GetResultTableSchema_DictAsync(sourceSql, sourceParameter));
                if (mapping == null || mapping.Count <= 0) throw new Exception("批量复制失败，无匹配列！");


                using var targetConnection = new OracleConnection(_targetConnectionString);
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();

                using var sourceConnection = new SqlConnection(_sourceConnectionString);
                using var cmd = _sqlHelper.createSqlCommand(sourceSql, sourceConnection, sourceParameter);
                using var reader = await cmd.ExecuteReaderAsync();
                var bulkCopy = new OracleBulkCopy(targetConnection);
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = targetTableName;
                foreach (var item in mapping)
                {
                    bulkCopy.ColumnMappings.Add(item);
                }
                bulkCopy.NotifyAfter = 1000;
                long totalnum = 0;
                bulkCopy.OracleRowsCopied += (s, e) =>
                {
                    totalnum += e.RowsCopied;
                    _logger.LogWarning($"BulkCopyToOracle {e.RowsCopied} ...");

                };
                bulkCopy.WriteToServer(reader);
                cmd.Parameters.Clear();
                return totalnum;
            }
            catch
            {
                throw;
            }
        }

        #endregion
    }
}
