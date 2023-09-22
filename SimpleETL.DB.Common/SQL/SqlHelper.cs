using Microsoft.Extensions.Logging;
using SimpleETL.DB.Common.Extension;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace SimpleETL.DB.Common.SQL
{
    /// <summary>
    /// Sql Server 查询帮助类
    /// By System.Data.SqlClient.dll
    /// </summary>
    public class SqlHelper : IDBHelper
    {

        public SqlHelper(ILogger logger, string connectionString)
        {
            _logger = logger;
            _connectionString = connectionString;
        }
        private ILogger _logger;
        private string _connectionString;
        public SqlHelper(string connectionString)
        {
            _connectionString = connectionString;
        }
        public DatabaseType DBType { get => DatabaseType.SqlServer; }

        #region CRUD

        public int Excute(string Sql)
        {
            return Excute(Sql, null);
        }
        public int Excute(string Sql, DbParameter[]? parameter)
        {
            try
            {
                using SqlConnection connection = new SqlConnection(_connectionString);
                using SqlCommand cmd = createSqlCommand(Sql, connection, parameter);
                var result = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return result;
            }
            catch (SqlException ex)
            {
                _logger.LogError(ex, $"SQL Error!\t{ex.BatchCommand?.CommandText};\t{ex.BatchCommand.Parameters.FormatParameters}");
                throw ex;
            }
            catch (Exception)
            {
                throw;
            }
        }
        public object ExcuteScalar(string Sql, DbParameter[]? parameter)
        {
            try
            {
                using SqlConnection connection = new SqlConnection(_connectionString);
                using SqlCommand cmd = createSqlCommand(Sql, connection, parameter);
                var result = cmd.ExecuteScalar();

                cmd.Parameters.Clear();
                return result;
            }
            catch (SqlException ex)
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
        public DataSet Query(string Sql, DbParameter[]? parameter)
        {
            try
            {
                using SqlConnection connection = new SqlConnection(_connectionString);
                using SqlCommand cmd = createSqlCommand(Sql, connection, parameter);
                using SqlDataAdapter da = new SqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                cmd.ExecuteReader(CommandBehavior.CloseConnection);
                da.Fill(ds);
                cmd.Parameters.Clear();
                return ds;
            }
            catch (SqlException ex)
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

        public async void BulkCopy(string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_connectionString);
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlBulkCopy bc = new SqlBulkCopy(connection);
                bc.DestinationTableName = targetTableName;
                GreanMapping(await GetTableSchemaDict(targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
                bc.WriteToServer(dtBulk);
            }
            catch
            {
                throw;
            }
        }
        public async void BulkCopyTrans(string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_connectionString);
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlTransaction transaction = connection.BeginTransaction();
                using SqlBulkCopy bc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
                bc.DestinationTableName = targetTableName;
                GreanMapping( await GetTableSchemaDict(targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
                try
                {
                    bc.WriteToServer(dtBulk);
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch
            {
                throw;
            }
        }
        public async void BulkCopyAsync(string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_connectionString);
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlBulkCopy bc = new SqlBulkCopy(connection);
                bc.DestinationTableName = targetTableName;
                GreanMapping( await GetTableSchemaDict(targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
                await bc.WriteToServerAsync(dtBulk);
            }
            catch
            {
                throw;
            }
        }
        public async void BulkCopyTransAsync(string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_connectionString);
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlTransaction transaction = connection.BeginTransaction();
                using SqlBulkCopy bc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
                bc.DestinationTableName = targetTableName;
                GreanMapping( await GetTableSchemaDict(targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
                try
                {
                    await bc.WriteToServerAsync(dtBulk);
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
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
        /// <remarks>see: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-schema-collections</remarks>
        public async Task<DataTable> GetTableSchemaSimpleAsync(string tablename)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                //您可以指定目录、模式、表名、列名来获取指定的列。您可以对 Column 使用四个限制，因此您应该创建一个 4 成员数组。
                string[] tableRestrictions = new string[4];
                //对于数组，0 - member代表Catalog； 1 - member 代表 Schema； // 2-member 代表表名； 3-member 代表列名。 // 现在我们指定要获取架构信息的列的 Table_Name 和 Column_Name。
                tableRestrictions[2] = tablename;
                return await connection.GetSchemaAsync("Tables", tableRestrictions);
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
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task<DataTable> GetTableSchemaAsync(string tableName)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                var sql = $"select * from {tableName}";
                using var cmd = createSqlCommand(sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var table = await reader.GetSchemaTableAsync();
                cmd.Parameters.Clear();
                return table;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        /// 获取表架构 字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetTableSchemaDict(string tableName)
        {
            return await GetTableSchema_DictAsync(tableName);
        }
        /// <summary>
        /// 获取表架构 字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetTableSchema_DictAsync(string tableName)
        {
            try
            {
                var dict = new Dictionary<int, string>();
                using var connection = new SqlConnection(_connectionString);
                var sql = $"select * from {tableName}";
                using var cmd = createSqlCommand(sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var schema = await reader.GetColumnSchemaAsync();
                for (int i = 0; i < schema.Count; i++)
                {
                    dict.Add(i, reader.GetName(i));
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
        ///  根据查询结果获取架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<DataTable> GetResultTableSchemaAsync(string Sql)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                using var cmd = createSqlCommand(Sql, connection);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var table = await reader.GetSchemaTableAsync();
                cmd.Parameters.Clear();
                return table;
            }
            catch (Exception)
            {
                throw;
            }
        }
        /// <summary>
        ///  根据查询结果获取架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public Dictionary<int, string> GetResultTableSchema_Dict(string Sql, SqlParameter[]? parameters = null)
        {
            return GetResultTableSchema_DictAsync(Sql, parameters).Result;
        }
        /// <summary>
        /// 根据查询结果获取架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetResultTableSchema_DictAsync(string Sql, SqlParameter[]? parameters = null)
        {
            try
            {
                var dict = new Dictionary<int, string>();
                using var connection = new SqlConnection(_connectionString);
                using var cmd = createSqlCommand(Sql, connection, parameters);
                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
                var schema = await reader.GetColumnSchemaAsync();
                for (int i = 0; i < schema.Count; i++)
                {
                    dict.Add(i, reader.GetName(i));
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

        #region SqlBulkCopy Mapping List

        public List<SqlBulkCopyColumnMapping> GreanMapping(Dictionary<int, string> targetSchema, DataTable dtBulk)
        {
            var list = new List<SqlBulkCopyColumnMapping>();
            try
            {
                if (dtBulk != null && dtBulk.Rows.Count > 0
                    && targetSchema != null && targetSchema.Count > 0)
                {
                    var column = dtBulk.Columns;

                    foreach (DataColumn item in column)
                    {
                        var t = targetSchema.Values.FirstOrDefault(p => p.ToUpper() == item.ColumnName.ToUpper());
                        if (!string.IsNullOrWhiteSpace(t))
                        {
                            list.Add(new(item.ColumnName, t));
                        }
                    }
                }
                return list;
            }
            catch (Exception)
            {

                throw;
            }
        }
        public List<SqlBulkCopyColumnMapping> GreanMapping(Dictionary<int, string> targetSchema, Dictionary<int, string> sourceSchema)
        {
            var list = new List<SqlBulkCopyColumnMapping>();
            try
            {
                if (sourceSchema != null && sourceSchema.Count > 0 && targetSchema != null && targetSchema.Count > 0)
                {
                    foreach (var item in sourceSchema)
                    {
                        var targetColumn = targetSchema.Values.FirstOrDefault(p => p.ToUpper() == item.Value.ToUpper());
                        if (!string.IsNullOrWhiteSpace(targetColumn))
                        {
                            list.Add(new(item.Value, targetColumn));
                        }
                    }
                }
                return list;
            }
            catch (Exception)
            {

                throw;
            }
        }
        #endregion

        public SqlCommand createSqlCommand(string sSql, SqlConnection connection, DbParameter[]? parameters = null, SqlTransaction? trans = null, int timeout = 3600)
        {

            if (connection.State != ConnectionState.Open)
                connection.Open();
            SqlCommand cmd = new SqlCommand()
            {
                Connection = connection,
                CommandText = sSql,
                CommandType = CommandType.Text,
                CommandTimeout = timeout
            };
            if (trans != null)
                cmd.Transaction = trans;

            if (parameters != null)
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
    }
}
