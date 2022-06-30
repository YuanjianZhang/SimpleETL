using DB.Model;
using Microsoft.EntityFrameworkCore;
using SimpleETL.DB.SQL.EntityFrameworkCore.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.SQL.EntityFrameworkCore
{
    public class DBContext_SQL : DbContext
    {

        private string? _connectionstring = string.Empty;
        public DBContext_SQL() : base()
        {
        }
        public DBContext_SQL(string connectionstring) : base()
        {
            _connectionstring = connectionstring;
        }

        /// <summary>
        /// 表的Schema
        /// </summary>
        [Obsolete("不再使用EF框架获取TableSchema ", true)]
        public DbSet<M_TableSchema_SQL> TableSchema { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
        }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!string.IsNullOrWhiteSpace(_connectionstring) && !optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseSqlServer(_connectionstring);
            }
        }
    }
}
