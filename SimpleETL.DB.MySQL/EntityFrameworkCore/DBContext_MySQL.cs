using Microsoft.EntityFrameworkCore;
using SimpleETL.DB.MySQL.EntityFrameworkCore.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.MySQL.EntityFrameworkCore
{
    public class DBContext_MySQL : DbContext
    {
        private string _connectionstring;

        public DBContext_MySQL()
        {
        }
        public DBContext_MySQL(string connectionstring)
        {
            _connectionstring = connectionstring;
        }
        /// <summary>
        /// 表的Schema
        /// </summary>
        [Obsolete("不再使用EF框架获取TableSchema ", true)]
        public DbSet<M_TableSchema_MySQL> TableSchema { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {

        }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!string.IsNullOrWhiteSpace(_connectionstring) && !optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseMySQL(_connectionstring);
            }
        }
    }
}
