using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;
using SimpleETL.DB.Common.Extension;
using System.Data;
using System.Data.Common;

namespace SimpleETL.DB.Common.MySQL
{
    public class OracleHelper : IDBHelper
    {
        public OracleHelper(ILogger logger, string connectionString)
        {
            _logger = logger;
            _connectionString = connectionString;
        }
        private readonly ILogger _logger;
        private string _connectionString;
        public DatabaseType DBType { get => DatabaseType.Oracle; }

        public int Excute(string Sql)
        {
            try
            {
                return Excute(Sql, null);
            }
            catch
            {
                throw;
            }
        }

        public int Excute(string Sql, DbParameter[]? parameter)
        {
            try
            {
                using OracleConnection connection = new OracleConnection(_connectionString);
                using OracleCommand cmd = createOracleCommand(Sql, connection, parameter);
                var result = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return result;
            }
            catch (OracleException ex)
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
                using OracleConnection connection = new OracleConnection(_connectionString);
                using OracleCommand cmd = createOracleCommand(Sql, connection, parameter);
                var result = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return result;
            }
            catch (OracleException ex)
            {
                _logger.LogError(ex, $"SQL Error!\t{ex.BatchCommand?.CommandText};\t{ex.BatchCommand.Parameters.FormatParameters}");
                throw ex;
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
                using OracleConnection connection = new OracleConnection(_connectionString);

                var Sql = $"SELECT * FROM {tablename} Where ROWNUM <= 1";
                //var Sql = "DESCRIBE @tablename";
                using OracleCommand cmd = createOracleCommand(Sql, connection);
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

        public DataSet Query(string Sql)
        {
            try
            {
                return Query(Sql, null);
            }
            catch
            {
                throw;
            }
        }

        public DataSet Query(string Sql, DbParameter[]? parameter)
        {
            try
            {
                using OracleConnection connection = new OracleConnection(_connectionString);
                using OracleCommand cmd = createOracleCommand(Sql, connection, parameter);
                using OracleDataAdapter da = new OracleDataAdapter(cmd);
                DataSet ds = new DataSet();
                cmd.ExecuteReader(CommandBehavior.CloseConnection);
                da.Fill(ds);
                cmd.Parameters.Clear();
                return ds;
            }
            catch (OracleException ex)
            {
                _logger.LogError(ex, $"SQL Error!\t{ex.BatchCommand?.CommandText};\t{ex.BatchCommand.Parameters.FormatParameters}");
                throw ex;
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// 创建MySqlCommand实例对象
        /// </summary>
        /// <param name="sSql"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        private OracleCommand createOracleCommand(string sSql, OracleConnection connection, DbParameter[]? parameters = null, OracleTransaction? trans = null, int timeout = 3600)
        {
            if (connection.State != ConnectionState.Open)
                connection.Open();

            OracleCommand cmd = new OracleCommand()
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
        /// 生成目标为MySQL的批量复制匹配映射关系
        /// </summary>
        /// <param name="dtBulk"></param>
        /// <returns></returns>
        public List<OracleBulkCopyColumnMapping> GreanMappingToOracle(Dictionary<int, string> TargetSchema, Dictionary<int, string> sourceSchema)
        {
            List<OracleBulkCopyColumnMapping> list = new List<OracleBulkCopyColumnMapping>();
            try
            {
                if (TargetSchema != null && TargetSchema.Count > 0 && sourceSchema != null && sourceSchema.Count > 0)
                {
                    foreach (var item in sourceSchema)
                    {
                        var targetColumn = TargetSchema.Values.FirstOrDefault(p => p.ToUpper() == item.Value.ToUpper());
                        if (!string.IsNullOrWhiteSpace(targetColumn))
                        {
                            var idx = sourceSchema.Keys.ToList().IndexOf(item.Key);
                            if (idx < 0) continue;
                            OracleBulkCopyColumnMapping m = new OracleBulkCopyColumnMapping(idx, targetColumn);
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
