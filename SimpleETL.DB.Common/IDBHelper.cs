using System.Data;
using System.Data.Common;

namespace SimpleETL.DB.Common
{
    /// <summary>
    /// 数据库帮助类通用方法定义接口
    /// </summary>
    public interface IDBHelper
    {
        DatabaseType DBType { get; }
        public int Excute(string Sql);


        public int Excute(string Sql, DbParameter[]? parameter);


        public object ExcuteScalar(string Sql, DbParameter[]? parameter);


        public DataSet Query(string Sql);


        public DataSet Query(string Sql, DbParameter[]? parameter);


        /// <summary>
        /// 获取表架构字典
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Sql"></param>
        /// <returns></returns>
        Task<Dictionary<int, string>> GetTableSchemaDict(string tablename);

    }
}