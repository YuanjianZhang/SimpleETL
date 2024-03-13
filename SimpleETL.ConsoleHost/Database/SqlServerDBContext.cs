using Microsoft.EntityFrameworkCore;
using SimpleETL.Util;

namespace SimpleETL.ConsoleHost.Database
{
    public class SqlServerDBContext : BaseDBContext<SqlServerDBContext>
    {
        public SqlServerDBContext(string connectionString) : base(connectionString) { }
        public SqlServerDBContext(DbContextOptions<SqlServerDBContext> options) : base(options) { }


        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .LogTo(GlobalConfig.DBContext_EnableLog ? Console.WriteLine : _ => { })
                    .EnableSensitiveDataLogging(GlobalConfig.DBContext_EnableSensitiveDataLog)
                    .EnableDetailedErrors(GlobalConfig.DBContext_EnableDetailedErrors)
                    .UseSqlServer(_connectionString);
            }
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
        }
    }
}
