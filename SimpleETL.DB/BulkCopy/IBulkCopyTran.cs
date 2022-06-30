using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.BulkCopy
{
    public interface IBulkCopyTran
    {
        int MSSQLToMySQL(string sourceKey, string targetKey, string targetTableName, string sourceSql, SqlParameter[]? sourceParameter);
        Task<int> MSSQLToMySQLAsync(string sourceKey, string targetKey, string targetTableName, string sourceSql, SqlParameter[]? sourceParameter);
        int MSSQLToMySQLTrans(string sourceKey, string targetKey, string targetTableName, string sourceSql, SqlParameter[]? sourceParameter);
        Task<int> MSSQLToMySQLTransAsync(string sourceKey, string targetKey, string targetTableName, string sourceSql, SqlParameter[]? sourceParameter);
    }
}
