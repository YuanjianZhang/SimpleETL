using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.ConsoleHost.Database
{
    public class MySQLDBContext:BaseDBContext<MySQLDBContext>
    {
        public MySQLDBContext(string connectionString) : base(connectionString) { }
        public MySQLDBContext(DbContextOptions<MySQLDBContext> options) : base(options) { }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseMySql(_connectionString, new MySqlServerVersion(new Version(8, 0, 31)));
            }
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {

            base.OnModelCreating(modelBuilder);
        }
    }
}
