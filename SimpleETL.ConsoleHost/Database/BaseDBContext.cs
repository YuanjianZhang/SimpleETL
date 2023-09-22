using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.ConsoleHost.Database
{
    public class BaseDBContext<T> : DbContext where T : DbContext
    {
        protected readonly string _connectionString;
        public BaseDBContext()
        {
        }
        protected BaseDBContext(string connectionString)
        {
            _connectionString = connectionString;
        }
        public BaseDBContext(DbContextOptions option) : base(option)
        {

        }
        public virtual DbSet<M_BulkCopyDemo> Demo { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            
            base.OnModelCreating(modelBuilder);
        }
    }
}
