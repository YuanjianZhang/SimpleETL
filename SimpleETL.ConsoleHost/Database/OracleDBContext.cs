using Microsoft.EntityFrameworkCore;
using SimpleETL.Util;

namespace SimpleETL.ConsoleHost.Database
{
    public class OracleDBContext : BaseDBContext<SqlServerDBContext>
    {
        public OracleDBContext(string connectionString) : base(connectionString) { }
        public OracleDBContext(DbContextOptions<OracleDBContext> options) : base(options) { }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .LogTo(GlobalConfig.DBContext_EnableLog ? Console.WriteLine : _ => { })
                    .EnableSensitiveDataLogging(GlobalConfig.DBContext_EnableSensitiveDataLog)
                    .EnableDetailedErrors(GlobalConfig.DBContext_EnableDetailedErrors)
                    .UseOracle(_connectionString);
            }
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {

            base.OnModelCreating(modelBuilder);
        }
    }
}
