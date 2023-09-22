using Microsoft.Extensions.Logging;
using MySqlConnector;
using SimpleETL.DB.Common.Extension;
using System.Data;
using System.Data.Common;

namespace SimpleETL.DB.Common.MySQL
{
    public class MySqlHelper : IDBHelper
    {
        public MySqlHelper(ILogger logger, string connectionString)
        {
            _logger = logger;
            _connectionString = connectionString;
        }
        private readonly ILogger _logger;
        private string _connectionString;
        public DatabaseType DBType { get => DatabaseType.MySQL; }

        #region CRUD
        public int Excute(string Sql)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_connectionString);
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
        public int Excute(string Sql, DbParameter[] parameter)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_connectionString);
                using MySqlCommand cmd = createMySqlCommand(Sql, connection, parameter);
                var result = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return result;
            }
            catch (MySqlException ex)
            {
                _logger.LogError(ex, $"SQL Error!\t{ex.BatchCommand?.CommandText};\t{ex.BatchCommand.Parameters.FormatParameters}");
                throw ex;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public object ExcuteScalar(string Sql, DbParameter[] parameter)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_connectionString);
                using MySqlCommand cmd = createMySqlCommand(Sql, connection, parameter);
                var result = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return result;
            }
            catch (MySqlException ex)
            {
                _logger.LogError(ex, $"SQL Error!\t{ex.BatchCommand?.CommandText};\t{ex.BatchCommand.Parameters.FormatParameters}");
                throw ex;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public DataSet Query(string Sql)
        {
            return Query(Sql, null);
        }
        public DataSet Query(string Sql, DbParameter[] parameter)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_connectionString);
                using MySqlCommand cmd = createMySqlCommand(Sql, connection, parameter);
                using MySqlDataAdapter da = new MySqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                cmd.ExecuteReader(CommandBehavior.CloseConnection);
                da.Fill(ds);
                cmd.Parameters.Clear();
                return ds;
            }
            catch (MySqlException ex)
            {
                _logger.LogError(ex, $"SQL Error!\t{ex.BatchCommand?.CommandText};\t{ex.BatchCommand.Parameters.FormatParameters}");
                throw ex;
            }
            catch (Exception)
            {
                throw;
            }
        }

        #endregion

        #region BulkCopy
        public int BulkCopy(string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_connectionString);
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

        public int BulkCopyTrans(string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_connectionString);
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

        public async Task<int> BulkCopyAsync(string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_connectionString);
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

        public async Task<int> BulkCopyTransAsync(string tableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");

                using MySqlConnection connection = new MySqlConnection(_connectionString);
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

        #endregion

        #region table schema
        /// <summary>
        /// 获取表架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public async Task<DataTable> GetTableSchemaAsync(string tablename)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_connectionString);

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
        public async Task<Dictionary<int, string>> GetTableSchemaDict(string tablename)
        {
            try
            {
                Dictionary<int, string> dict = new();
                using MySqlConnection connection = new MySqlConnection(_connectionString);

                var Sql = $"SELECT * FROM {tablename} Limit 1";
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
        public async Task<DataTable> GetResultTableSchemaAsync(string Sql)
        {
            try
            {
                using MySqlConnection connection = new MySqlConnection(_connectionString);
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
        public async Task<Dictionary<int, string>> GetResultTableSchemaDict(string Sql)
        {
            try
            {
                Dictionary<int, string> dict = new();
                using MySqlConnection connection = new MySqlConnection(_connectionString);
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
        private MySqlCommand createMySqlCommand(string sSql, MySqlConnection connection, DbParameter[]? parameters = null, MySqlTransaction? trans = null, int timeout = 3600)
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
                cmd.Parameters.Clear();
                parameters = DbParameterExtension.ToDbParameter(DBType, parameters);
                foreach (var parameter in parameters)
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
        /// 生成目标为MySQL的批量复制匹配映射关系
        /// </summary>
        /// <param name="dtBulk"></param>
        /// <returns></returns>
        public List<MySqlBulkCopyColumnMapping> GreanMappingToMySQL(Dictionary<int, string> mysqlTargetSchema, Dictionary<int, string> sourceSchema)
        {
            List<MySqlBulkCopyColumnMapping> list = new List<MySqlBulkCopyColumnMapping>();
            try
            {
                if (mysqlTargetSchema != null && mysqlTargetSchema.Count > 0 && sourceSchema != null && sourceSchema.Count > 0)
                {

                    foreach (var item in sourceSchema)
                    {
                        var targetColumn = mysqlTargetSchema.Values.FirstOrDefault(p => p.ToUpper() == item.Value.ToUpper());
                        if (!string.IsNullOrWhiteSpace(targetColumn))
                        {
                            var idx = sourceSchema.Keys.ToList().IndexOf(item.Key);
                            if (idx < 0) continue;
                            MySqlBulkCopyColumnMapping m = new MySqlBulkCopyColumnMapping(idx, targetColumn);
                            list.Add(m);
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
