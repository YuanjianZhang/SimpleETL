using Microsoft.EntityFrameworkCore;
using SimpleETL.Util;

namespace SimpleETL.ConsoleHost.Database
{
    public class MySQLDBContext : BaseDBContext<MySQLDBContext>
    {
        public MySQLDBContext(string connectionString) : base(connectionString) { }
        public MySQLDBContext(DbContextOptions<MySQLDBContext> options) : base(options) { }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .LogTo(GlobalConfig.DBContext_EnableLog ? Console.WriteLine : _ => { })
                    .EnableSensitiveDataLogging(GlobalConfig.DBContext_EnableSensitiveDataLog)
                    .EnableDetailedErrors(GlobalConfig.DBContext_EnableDetailedErrors)
                    .UseMySql(_connectionString, new MySqlServerVersion(new Version(8, 0, 31)));
            }
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {

            base.OnModelCreating(modelBuilder);
        }
    }
}
