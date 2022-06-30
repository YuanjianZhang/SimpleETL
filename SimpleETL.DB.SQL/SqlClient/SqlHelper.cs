using SimpleETL.DB.Common;
using System.Data;
using System.Data.SqlClient;

namespace SimpleETL.DB.SQL.SqlClient
{
    /// <summary>
    /// Sql Server 查询帮助类
    /// By System.Data.SqlClient.dll
    /// </summary>
    public class SqlHelper
    {

        private readonly IDBCommon _dbCommon;
        public SqlHelper(IDBCommon DBCommon)
        {
            _dbCommon = DBCommon;
        }

        public int Excute(string Key, string Sql)
        {
            try
            {
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                using SqlCommand cmd = createSqlCommand(Sql, connection);
                return cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {
                throw;
            }
        }
        public int Excute(string Key, string Sql, SqlParameter[] parameter)
        {
            try
            {
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                using SqlCommand cmd = createSqlCommand(Sql, connection, parameter);
                return cmd.ExecuteNonQuery();
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
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                using SqlCommand cmd = createSqlCommand(Sql, connection);
                using SqlDataAdapter da = new SqlDataAdapter(cmd);
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
        public DataSet Query(string Key, string Sql, SqlParameter[] parameter)
        {
            try
            {
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                using SqlCommand cmd = createSqlCommand(Sql, connection, parameter);
                using SqlDataAdapter da = new SqlDataAdapter(cmd);
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

        public void BulkCopy(string Key, string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlBulkCopy bc = new SqlBulkCopy(connection);
                bc.DestinationTableName = targetTableName;
                GreanMapping(GetTableSchema_Dict(Key, targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
                bc.WriteToServer(dtBulk);
            }
            catch
            {
                throw;
            }
        }
        public void BulkCopyTrans(string Key, string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlTransaction transaction = connection.BeginTransaction();
                using SqlBulkCopy bc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
                bc.DestinationTableName = targetTableName;
                GreanMapping(GetTableSchema_Dict(Key, targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
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
        public async void BulkCopyAsync(string Key, string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlBulkCopy bc = new SqlBulkCopy(connection);
                bc.DestinationTableName = targetTableName;
                GreanMapping(GetTableSchema_Dict(Key, targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
                await bc.WriteToServerAsync(dtBulk);
            }
            catch
            {
                throw;
            }
        }
        public async void BulkCopyTransAsync(string Key, string targetTableName, DataTable dtBulk)
        {
            try
            {
                if (dtBulk == null || dtBulk.Rows.Count <= 0) throw new Exception("源数据表不能为空！");
                using SqlConnection connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (connection.State != ConnectionState.Open) connection.Open();
                using SqlTransaction transaction = connection.BeginTransaction();
                using SqlBulkCopy bc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
                bc.DestinationTableName = targetTableName;
                GreanMapping(GetTableSchema_Dict(Key, targetTableName), dtBulk).ForEach(x => bc.ColumnMappings.Add(x));
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
        public void BulkCopyWithDiffDB(string Key, string targetTableName, string sourceKey, string sourceSql, SqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return;

                using SqlConnection targetConnection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var sourceConnection = new SqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createSqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = sourceCmd.ExecuteReader();
                using SqlBulkCopy bc = new SqlBulkCopy(targetConnection);
                bc.DestinationTableName = targetTableName;
                mapping.ForEach(x => bc.ColumnMappings.Add(x));
                bc.WriteToServer(sourceReader);
                sourceCmd.Parameters.Clear();
            }
            catch
            {
                throw;
            }
        }
        public async void BulkCopyWithDiffDBAsync(string Key, string targetTableName, string sourceKey, string sourceSql, SqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return;

                using SqlConnection targetConnection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var sourceConnection = new SqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createSqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = await sourceCmd.ExecuteReaderAsync();
                using SqlBulkCopy bc = new SqlBulkCopy(targetConnection);
                bc.DestinationTableName = targetTableName;
                mapping.ForEach(x => bc.ColumnMappings.Add(x));
                await bc.WriteToServerAsync(sourceReader);
                sourceCmd.Parameters.Clear();
            }
            catch
            {
                throw;
            }
        }
        public void BulkCopyWithDiffDBTrans(string Key, string targetTableName, string sourceKey, string sourceSql, SqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return;

                using SqlConnection targetConnection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var trans = targetConnection.BeginTransaction();
                using var sourceConnection = new SqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createSqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = sourceCmd.ExecuteReader();
                try
                {
                    using SqlBulkCopy bc = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.Default, trans);
                    bc.DestinationTableName = targetTableName;
                    mapping.ForEach(x => bc.ColumnMappings.Add(x));
                    bc.WriteToServer(sourceReader);
                    trans.Commit();
                    sourceCmd.Parameters.Clear();
                }
                catch (Exception)
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
        public async void BulkCopyWithDiffDBTransAsync(string Key, string targetTableName, string sourceKey, string sourceSql, SqlParameter[] parameters)
        {
            try
            {
                var mapping = GreanMapping(GetTableSchema_Dict(Key, targetTableName), GetResultTableSchema_Dict(sourceKey, sourceSql));
                if (mapping == null || mapping.Count <= 0) return;

                using SqlConnection targetConnection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                if (targetConnection.State != ConnectionState.Open) targetConnection.Open();
                using var trans = targetConnection.BeginTransaction();
                using var sourceConnection = new SqlConnection(_dbCommon.GetConnectionString(sourceKey));
                using var sourceCmd = createSqlCommand(sourceSql, sourceConnection, parameters);
                using var sourceReader = await sourceCmd.ExecuteReaderAsync();
                try
                {
                    using SqlBulkCopy bc = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.Default, trans);
                    bc.DestinationTableName = targetTableName;
                    mapping.ForEach(x => bc.ColumnMappings.Add(x));
                    await bc.WriteToServerAsync(sourceReader);
                    await trans.CommitAsync();
                    sourceCmd.Parameters.Clear();
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
        /// 获取表架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        /// <remarks>see: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-schema-collections</remarks>
        public DataTable GetTableSchemaSimple(string Key, string tablename)
        {
            try
            {
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                //您可以指定目录、模式、表名、列名来获取指定的列。您可以对 Column 使用四个限制，因此您应该创建一个 4 成员数组。
                string[] tableRestrictions = new string[4];
                //对于数组，0 - member代表Catalog； 1 - member 代表 Schema； // 2-member 代表表名； 3-member 代表列名。 // 现在我们指定要获取架构信息的列的 Table_Name 和 Column_Name。
                tableRestrictions[2] = tablename;
                return connection.GetSchema("Tables", tableRestrictions);
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
        /// <remarks>see: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-schema-collections</remarks>
        public async Task<DataTable> GetTableSchemaSimpleAsync(string Key, string tablename)
        {
            try
            {
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
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
        public DataTable GetTableSchema(string Key, string tableName)
        {
            try
            {
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                var sql = $"select * from {tableName}";
                using var cmd = createSqlCommand(sql, connection);
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
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task<DataTable> GetTableSchemaAsync(string Key, string tableName)
        {
            try
            {
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
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
        public Dictionary<int, string> GetTableSchema_Dict(string Key, string tableName)
        {
            try
            {
                var dict = new Dictionary<int, string>();
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                var sql = $"select * from {tableName}";
                using var cmd = createSqlCommand(sql, connection);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                var schema = reader.GetColumnSchema();
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
        /// 获取表架构 字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetTableSchema_DictAsync(string Key, string tableName)
        {
            try
            {
                var dict = new Dictionary<int, string>();
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
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
        /// 获取查询结果 架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public DataTable GetResultTableSchema(string Key, string Sql)
        {
            try
            {
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                using var cmd = createSqlCommand(Sql, connection);
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
        /// 获取查询结果 架构
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<DataTable> GetResultTableSchemaAsync(string Key, string Sql)
        {
            try
            {
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
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
        /// 获取查询结果 架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public Dictionary<int, string> GetResultTableSchema_Dict(string Key, string Sql, SqlParameter[]? parameters = null)
        {
            try
            {
                var dict = new Dictionary<int, string>();
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
                using var cmd = createSqlCommand(Sql, connection, parameters);
                using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                var schema = reader.GetColumnSchema();
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
        /// 获取查询结果 架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public async Task<Dictionary<int, string>> GetResultTableSchema_DictAsync(string Key, string Sql, SqlParameter[]? parameters = null)
        {
            try
            {
                var dict = new Dictionary<int, string>();
                using var connection = new SqlConnection(_dbCommon.GetConnectionString(Key));
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

        public List<SqlBulkCopyColumnMapping> GreanMapping(Dictionary<int, string> targetSchema, DataTable dtBulk)
        {
            var list = new List<SqlBulkCopyColumnMapping>();
            try
            {
                if (dtBulk != null && dtBulk.Rows.Count > 0 && targetSchema != null && targetSchema.Count > 0)
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
        public SqlCommand createSqlCommand(string sSql, SqlConnection connection, SqlParameter[]? cmdParamter = null, SqlTransaction? trans = null, int timeout = 3600)
        {

            if (connection.State != ConnectionState.Open)
                connection.Open();
            SqlCommand cmd = new SqlCommand()
            {
                Connection = connection,
                CommandText = sSql,
                CommandType = CommandType.Text,
                CommandTimeout = timeout,
            };
            if (trans != null) cmd.Transaction = trans;
            if (cmdParamter != null)
            {
                foreach (SqlParameter parameter in cmdParamter)
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
