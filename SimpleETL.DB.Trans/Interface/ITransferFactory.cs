using SimpleETL.DB.Common;

namespace SimpleETL.DB.Trans.Interface
{
    /// <summary>
    /// 数据库数据传输工厂类
    /// </summary>
    public interface ITransferFactory
    {
        /// <summary>
        /// 源数据库类型
        /// </summary>
        DatabaseType SourceType { get; set; }
        /// <summary>
        /// 目标数据库类型
        /// </summary>
        DatabaseType TargetType { get; set; }
        /// <summary>
        /// 源数据库链接字符串
        /// </summary>
        string SourceConStr { get; set; }
        /// <summary>
        /// 目标数据库连接字符串
        /// </summary>
        string TargetConStr { get; set; }
        /// <summary>
        /// 数据传输事务实现对象
        /// </summary>
        /// <returns></returns>
        IDBTransfer GetDBTransfer();

    }
}
