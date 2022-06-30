using Microsoft.Extensions.Logging;
using MySqlConnector;
using SimpleETL.DB.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.MySQL.MySqlConnector
{
    public class MySqlHelper
    {
        private readonly IDBCommon _dbCommon;
        private readonly ILogger<MySqlHelper> _logger;

        public MySqlHelper(IDBCommon dBCommon, ILogger<MySqlHelper> logger)
        {
            _dbCommon = dBCommon;
            _logger = logger;
        }

        public int Excute(string Key, string Sql)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                var result = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return result;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public int Excute(string Key, string Sql, MySqlParameter[] parameter)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection, parameter);
                var result = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return result;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Query(string Key, string Sql)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                cmd.ExecuteReader(CommandBehavior.CloseConnection);
                da.Fill(ds);
                cmd.Parameters.Clear();
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Query(string Key, string Sql, MySqlParameter[] parameter)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection, parameter);
                using MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                cmd.ExecuteReader(CommandBehavior.CloseConnection);
                da.Fill(ds);
                cmd.Parameters.Clear();
                return ds;
            }
            catch (Exception)
            {
                throw;
            }
        }

        #region BulkCopy
        public int BulkCopy(string Key, string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                MySqlBulkCopy bc = new MySqlBulkCopy(connection);
                bc.DestinationTableName = tableName;
                bc.ColumnMappings.AddRange(GreanMapping(dtBulk));
                var result = bc.WriteToServer(dtBulk);

                if (result.Warnings.Count != 0)
                {
                    _logger.LogWarning("BulkCopy数据警告", result.Warnings);
                }
                return result.RowsInserted;
            }
            catch
            {
                throw;
            }
        }

        public int BulkCopyTrans(string Key, string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                using var trans = connection.BeginTransaction();
                MySqlBulkCopy bc = new MySqlBulkCopy(connection, trans);
                bc.DestinationTableName = tableName;
                bc.ColumnMappings.AddRange(GreanMapping(dtBulk));
                try
                {
                    var result = bc.WriteToServer(dtBulk);
                    trans.Commit();
                    if (result.Warnings.Count != 0)
                    {
                        _logger.LogWarning("MySqlBulkCopy警告", result.Warnings);
                    }
                    return result.RowsInserted;
                }
                catch
                {
                    trans.Rollback();
                    throw;
                }
            }
            catch
            {
                throw;
            }
        }

        public async Task<int> BulkCopyAsync(string Key, string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                MySqlBulkCopy bc = new MySqlBulkCopy(connection);
                bc.DestinationTableName = tableName;
                bc.ColumnMappings.AddRange(GreanMapping(dtBulk));
                var result = await bc.WriteToServerAsync(dtBulk);

                if (result.Warnings.Count != 0)
                {
                    _logger.LogWarning("BulkCopy数据警告", result.Warnings);
                }
                return result.RowsInserted;
            }
            catch
            {
                throw;
            }
        }

        public async Task<int> BulkCopyTransAsync(string Key, string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                using var trans = connection.BeginTransaction();
                MySqlBulkCopy bc = new MySqlBulkCopy(connection, trans);
                bc.DestinationTableName = tableName;
                bc.ColumnMappings.AddRange(GreanMapping(dtBulk));
                try
                {
                    var result = await bc.WriteToServerAsync(dtBulk);
                    await trans.CommitAsync();
                    if (result.Warnings.Count != 0)
                    {
                        _logger.LogWarning("MySqlBulkCopy警告", result.Warnings);
                    }
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

        public int BulkCopyWithDiffDB(string Key, string targetTableName, string sourceKey, string sourceSql, MySqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return 0;

                using MySqlConnection targetConnection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var sourceConnection = new MySqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createMySqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = sourceCmd.ExecuteReader();
                MySqlBulkCopy bc = new MySqlBulkCopy(targetConnection);
                bc.DestinationTableName = targetTableName;
                bc.ColumnMappings.AddRange(mapping);

                var result = bc.WriteToServer(sourceReader);
                if (result.Warnings.Count != 0)
                {
                    _logger.LogWarning("MySqlBulkCopy警告", result.Warnings);
                }
                sourceCmd.Parameters.Clear();
                return result.RowsInserted;
            }
            catch
            {
                throw;
            }
        }
        public async Task<int> BulkCopyWithDiffDBAsync(string Key, string targetTableName, string sourceKey, string sourceSql, MySqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return 0;

                using MySqlConnection targetConnection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();

                using var sourceConnection = new MySqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createMySqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = await sourceCmd.ExecuteReaderAsync();
                MySqlBulkCopy bc = new MySqlBulkCopy(targetConnection);
                bc.DestinationTableName = targetTableName;
                bc.ColumnMappings.AddRange(mapping);

                var result = await bc.WriteToServerAsync(sourceReader);
                if (result.Warnings.Count != 0)
                {
                    _logger.LogWarning("MySqlBulkCopy警告", result.Warnings);
                }
                sourceCmd.Parameters.Clear();
                return result.RowsInserted;
            }
            catch
            {
                throw;
            }
        }
        public int BulkCopyWithDiffDBTrans(string Key, string targetTableName, string sourceKey, string sourceSql, MySqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return 0;

                using MySqlConnection targetConnection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var trans = targetConnection.BeginTransaction();
                using var sourceConnection = new MySqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createMySqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = sourceCmd.ExecuteReader();
                try
                {
                    MySqlBulkCopy bc = new MySqlBulkCopy(targetConnection, trans);
                    bc.DestinationTableName = targetTableName;
                    bc.ColumnMappings.AddRange(mapping);
                    var result = bc.WriteToServer(sourceReader);
                    trans.Commit();
                    if (result.Warnings.Count != 0)
                    {
                        _logger.LogWarning("MySqlBulkCopy警告", result.Warnings);
                    }
                    sourceCmd.Parameters.Clear();
                    return result.RowsInserted;
                }
                catch
                {
                    trans.Rollback();
                    throw;
                }
            }
            catch
            {
                throw;
            }
        }
        public async Task<int> BulkCopyWithDiffDBTransAsync(string Key, string targetTableName, string sourceKey, string sourceSql, MySqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return 0;

                using MySqlConnection targetConnection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var trans = targetConnection.BeginTransaction();
                using var sourceConnection = new MySqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createMySqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = await sourceCmd.ExecuteReaderAsync();
                try
                {
                    MySqlBulkCopy bc = new MySqlBulkCopy(targetConnection, trans);
                    bc.DestinationTableName = targetTableName;
                    bc.ColumnMappings.AddRange(mapping);
                    var result = await bc.WriteToServerAsync(sourceReader);
                    await trans.CommitAsync();
                    if (result.Warnings.Count != 0)
                    {
                        _logger.LogWarning("MySqlBulkCopy警告", result.Warnings);
                    }
                    sourceCmd.Parameters.Clear();
                    return result.RowsInserted;
                }
                catch (Exception)
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
        #endregion

        #region table schema

        /// <summary>
        /// 使用SHOW COLUMNS / DESCRIBE 语句获取表架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tablename"></param>
        /// <remarks>https://dev.mysql.com/doc/refman/8.0/en/show-columns.html</remarks>
        /// <returns></returns>
        [Obsolete("不建议该方法")]
        public DataTable GetTableSchemaByDBStatements(string Key, string tablename)
        {
            try
            {
                //var Sql = "DESCRIBE @tablename";
                var Sql = "SHOW FULL COLUMNS FROM @tablename";
                return Query(Key, Sql, new MySqlParameter[] { new MySqlParameter("@tablename", tablename) }).Tables[0];
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取表架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public DataTable GetTableSchema(string Key, string tablename)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));

                var Sql = $"SELECT * FROM {tablename}";
                //var Sql = "DESCRIBE @tablename";
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                var table = reader.GetSchemaTable();
                cmd.Parameters.Clear();
                return table;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取表架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public async Task<DataTable> GetTableSchemaAsync(string Key, string tablename)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));

                var Sql = $"SELECT * FROM {tablename}";
                //var Sql = "DESCRIBE @tablename";
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var table = reader.GetSchemaTable();
                cmd.Parameters.Clear();
                return table;
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// 获取表架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public Dictionary<int, string> GetTableSchema_Dict(string Key, string tablename)
        {
            try
            {
                Dictionary<int, string> dict = new();
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));

                var Sql = $"SELECT * FROM {tablename}";
                //var Sql = "DESCRIBE @tablename";
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                var schema = reader.GetColumnSchema();
                for (int i = 0; i < schema.Count; i++)
                {
                    var destinationColumn = reader.GetName(i);
                    dict.Add(i, destinationColumn);
                }
                cmd.Parameters.Clear();
                return dict;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取表架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetTableSchema_DictAsync(string Key, string tablename)
        {
            try
            {
                Dictionary<int, string> dict = new();
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));

                var Sql = $"SELECT * FROM {tablename}";
                //var Sql = "DESCRIBE @tablename";
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var schema = await reader.GetColumnSchemaAsync();
                for (int i = 0; i < schema.Count; i++)
                {
                    var destinationColumn = reader.GetName(i);
                    dict.Add(i, destinationColumn);
                }
                cmd.Parameters.Clear();
                return dict;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取查询结果的TableSchema
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public DataTable GetResultTableSchema(string Key, string Sql)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                var table = reader.GetSchemaTable();
                cmd.Parameters.Clear();
                return table;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取查询结果的TableSchema
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<DataTable> GetResultTableSchemaAsync(string Key, string Sql)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var table = reader.GetSchemaTable();
                cmd.Parameters.Clear();
                return table;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取查询结果的TableSchema 字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public Dictionary<int, string> GetResultTableSchema_Dict(string Key, string Sql)
        {
            try
            {
                Dictionary<int, string> dict = new();
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                var schema = reader.GetColumnSchema();
                for (int i = 0; i < schema.Count; i++)
                {
                    var destinationColumn = reader.GetName(i);
                    dict.Add(i, destinationColumn);
                }
                cmd.Parameters.Clear();
                return dict;
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// 获取查询结果的TableSchema 字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetResultTableSchema_DictAsync(string Key, string Sql)
        {
            try
            {
                Dictionary<int, string> dict = new();
                using MySqlConnection connection = new MySqlConnection(_dbCommon.GetConnectionString(Key));
                using MySqlCommand cmd = createMySqlCommand(Sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var schema = reader.GetColumnSchema();
                for (int i = 0; i < schema.Count; i++)
                {
                    var destinationColumn = reader.GetName(i);
                    dict.Add(i, destinationColumn);
                }
                cmd.Parameters.Clear();
                return dict;
            }
            catch (Exception)
            {
                throw;
            }
        }

        #endregion

        /// <summary>
        /// 创建MySqlCommand实例对象
        /// </summary>
        /// <param name="sSql"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        private static MySqlCommand createMySqlCommand(string sSql, MySqlConnection connection, MySqlParameter[]? parameters = null, MySqlTransaction? trans = null, int timeout = 3600)
        {
            if (connection.State != ConnectionState.Open)
                connection.Open();

            MySqlCommand cmd = new MySqlCommand()
            {
                Connection = connection,
                CommandText = sSql,
                CommandType = CommandType.Text,
                CommandTimeout = timeout
            };
            if (trans != null)
                cmd.Transaction = trans;

            if (parameters != null && parameters.Length > 0)
            {
                foreach (MySqlParameter parameter in parameters)
                {
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        parameter.Value == null)
                    {
                        parameter.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(parameter);
                }
            }

            return cmd;
        }

        /// <summary>
        /// 生成批量复制匹配映射，通过DataTable
        /// </summary>
        /// <param name="dtBulk"></param>
        /// <returns></returns>
        public List<MySqlBulkCopyColumnMapping> GreanMapping(DataTable dtBulk)
        {
            List<MySqlBulkCopyColumnMapping> list = new List<MySqlBulkCopyColumnMapping>();
            try
            {
                if (dtBulk != null && dtBulk.Rows.Count > 0)
                {
                    var columns = dtBulk.Columns;
                    if (columns != null && columns.Count > 0)
                    {
                        int idx = 0;
                        foreach (DataColumn item in columns)
                        {
                            MySqlBulkCopyColumnMapping m = new MySqlBulkCopyColumnMapping(idx, item.ColumnName);
                            list.Add(m);
                            idx++;
                        }
                    }
                }
                return list;
            }
            catch (Exception)
            {
                return list;
            }
        }
        /// <summary>
        /// 生成批量复制匹配映射，通过DataTable
        /// </summary>
        /// <param name="dtBulk"></param>
        /// <returns></returns>
        public List<MySqlBulkCopyColumnMapping> GreanMapping(Dictionary<int, string> sourceSchema, Dictionary<int, string> targetSchema)
        {
            List<MySqlBulkCopyColumnMapping> list = new List<MySqlBulkCopyColumnMapping>();
            try
            {
                if (sourceSchema != null && sourceSchema.Count > 0 && targetSchema != null && targetSchema.Count > 0)
                {
                    int idx = 0;
                    foreach (var item in sourceSchema)
                    {
                        var targetColumn = targetSchema.Values.FirstOrDefault(p => p.ToUpper() == item.Value.ToUpper());
                        if (!string.IsNullOrWhiteSpace(targetColumn))
                        {
                            MySqlBulkCopyColumnMapping m = new MySqlBulkCopyColumnMapping(idx, targetColumn);
                            list.Add(m);
                            idx++;
                        }
                    }
                }
                return list;
            }
            catch (Exception)
            {
                return list;
            }
        }


    }
}
