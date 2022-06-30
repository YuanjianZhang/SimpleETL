using DB.Model;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using SimpleETL.DB.SQL.EntityFrameworkCore.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.SQL.EntityFrameworkCore
{

    public class SqlEntityHelper
    {
        private readonly DBContext_SQL _dbContext;
        public SqlEntityHelper(DBContext_SQL dbContext)
        {
            _dbContext = dbContext;
        }

        /// <summary>
        /// 获取TableSchema
        /// </summary>
        /// <param name="tablename"></param>
        /// <returns></returns>
        [Obsolete("不再使用EF框架获取TableSchema ", true)]
        public List<M_TableSchema_SQL> GetSqlTableSchema(string tablename)
        {
            try
            {
                var sql = @"SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = @tablename";

                var param = new SqlParameter("@tablename", tablename);

                var schematable = _dbContext.TableSchema
                .FromSqlRaw(sql, param)
                .ToList();

                return schematable;
            }
            catch (Exception ex)
            {

                throw;
            }
        }

    }
}
