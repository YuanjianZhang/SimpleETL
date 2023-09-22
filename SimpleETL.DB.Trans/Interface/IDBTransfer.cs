using SimpleETL.DB.Common;
using System.Data.SqlClient;

namespace SimpleETL.DB.Trans.Interface
{
    /// <summary>
    /// 定义数据库数据传输/推送的接口
    /// </summary>
    public interface IDBTransfer
    {
        /// <summary>
        /// 源数据库类型
        /// </summary>
        DatabaseType SouceDBType { get; }
        /// <summary>
        /// 目标数据库类型
        /// </summary>
        DatabaseType TargetDBType { get; set; }
        /// <summary>
        /// 批量复制
        /// </summary>
        /// <param name="sourceSql">源数据SQL</param>
        /// <param name="targetTableName">目标库的目标表</param>
        /// <param name="sourceParameter">源数据SQL参数</param>
        /// <returns></returns>
        Task<long> BulkCopy(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter);
        /// <summary>
        /// 批量复制【事务】
        /// </summary>
        /// <param name="sourceSql">源数据SQL</param>
        /// <param name="targetTableName">目标库的目标表</param>
        /// <param name="sourceParameter">源数据SQL参数</param>
        /// <returns></returns>
        Task<int> BulkCopyTrans(string sourceSql, string targetTableName, SqlParameter[]? sourceParameter);

    }
}
