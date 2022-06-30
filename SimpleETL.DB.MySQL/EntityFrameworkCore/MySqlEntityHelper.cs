using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using SimpleETL.DB.MySQL.EntityFrameworkCore.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.MySQL.EntityFrameworkCore
{
    public class MySqlEntityHelper
    {
        private readonly DBContext_MySQL _dbContext;
        public MySqlEntityHelper(DBContext_MySQL dbContext)
        {
            _dbContext = dbContext;
        }

        /// <summary>
        /// 获取TableSchema
        /// </summary>
        /// <param name="tablename"></param>
        /// <returns></returns>
        [Obsolete("不再使用EF框架获取TableSchema ", true)]
        public List<M_TableSchema_MySQL> GetSqlTableSchema(string tablename)
        {
            try
            {
                var sql = $"SHOW FULL COLUMNS FROM {tablename}";

                var schematable = _dbContext.TableSchema
                .FromSqlRaw(sql)
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
