using MySqlConnector;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.Common.Extension
{

    public static class DbParameterExtension
    {
        /// <summary>
        /// 根据配置文件中所配置的数据库类型
        /// 来创建相应数据库的参数对象
        /// </summary>
        /// <returns></returns>
        public static DbParameter CreateDbParameter(DatabaseType dbtype)
        {
            switch (dbtype)
            {
                case DatabaseType.SqlServer:
                    return new SqlParameter();

                case DatabaseType.MySQL:
                    return new MySqlParameter();
                default:
                    throw new Exception("数据库类型目前不支持！");
            }
        }

        /// <summary>
        /// 根据配置文件中所配置的数据库类型
        /// 来创建相应数据库的参数对象
        /// </summary>
        /// <returns></returns>
        public static DbParameter CreateDbParameter(DatabaseType dbtype, string paramName, object value)
        {
            DbParameter param = CreateDbParameter(dbtype);
            param.ParameterName = paramName;
            param.Value = value;
            return param;
        }

        /// <summary>
        /// 转换对应的数据库参数
        /// </summary>
        /// <param name="dbParameter">参数</param>
        /// <returns></returns>
        public static DbParameter[] ToDbParameter(DatabaseType dbtype, DbParameter[] dbParameter)
        {
            int i = 0;
            int size = dbParameter.Length;
            DbParameter[] _dbParameter = null;
            switch (dbtype)
            {
                case DatabaseType.SqlServer:
                    _dbParameter = new SqlParameter[size];
                    while (i < size)
                    {
                        _dbParameter[i] = new SqlParameter(dbParameter[i].ParameterName, dbParameter[i].Value);
                        i++;
                    }
                    break;
                case DatabaseType.MySQL:
                    _dbParameter = new MySqlParameter[size];
                    while (i < size)
                    {
                        _dbParameter[i] = new MySqlParameter(dbParameter[i].ParameterName, dbParameter[i].Value);
                        i++;
                    }
                    break;
                default:
                    throw new Exception("数据库类型目前不支持！");
            }
            return _dbParameter;
        }

        public static string FormatParameters(this DbParameterCollection parameters, bool logParameterValues)
        {
            var result = parameters.Cast<DbParameter>().Select(p =>
            {
                var builder = new StringBuilder();
                builder.AppendLine(p.ParameterName + "=" + (logParameterValues ? p.Value?.ToString() : "?"));
                return builder.ToString();
            });
            return string.Join(string.Empty,result);
        }
    }
}