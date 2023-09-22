using SimpleETL.DB.Common;

namespace SimpleETL.DB.Trans.Interface
{
    /// <summary>
    /// 数据库数据传输工厂类
    /// </summary>
    public interface ITransferFactory
    {
        /// <summary>
        /// 数据传输事务实现对象
        /// </summary>
        /// <returns></returns>
        IDBTransfer GetDBTransfer(DatabaseType sourceType, string sourceStr, DatabaseType targetType, string targetStr);

    }
}
