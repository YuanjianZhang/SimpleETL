using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using SimpleETL.Business;
using SimpleETL.ConsoleHost.Database;
using SimpleETL.DB.Common;
using SimpleETL.DB.Common.SQL;
using SimpleETL.DB.Trans;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Text;
using System.Transactions;

namespace SimpleETL.ConsoleHost.Jobs
{
    public class DataTransDemoJob : IJob
    {
        private readonly IConfiguration _config;
        private readonly ILogger<DataTransDemoJob> logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly OracleDBContext _oracleDBContext;
        private readonly SqlServerDBContext _sqlServerDBContext;
        private readonly MySQLDBContext _mySQLDBContext;

        public DataTransDemoJob(ILoggerFactory loggerFactory, ILogger<DataTransDemoJob> logger, IConfiguration configuration, OracleDBContext? oracleDBContext = null, SqlServerDBContext? sqlServerDBContext = null, MySQLDBContext? mySQLDBContext = null)
        {
            this.logger = logger;
            this.loggerFactory = loggerFactory;
            this._oracleDBContext = oracleDBContext;
            this._sqlServerDBContext = sqlServerDBContext;
            this._mySQLDBContext = mySQLDBContext;
            this._config = configuration;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                var source = context.JobDetail.JobDataMap.Get("source") as string;
                var target = context.JobDetail.JobDataMap.Get("target") as string;

                
                logger.LogInformation("load schema");
                _oracleDBContext?.Database.EnsureCreated();
                _sqlServerDBContext?.Database.EnsureCreated();
                _mySQLDBContext?.Database.EnsureCreated();

                if (_sqlServerDBContext.Demo.Count()<=0)
                {
                    logger.LogInformation($"load test data :{source}");
                    LoadTestTableData(source, 10000);
                }

                logger.LogInformation("reset target table;");
                ResetTable(target, typeof(M_BulkCopyDemo));

                logger.LogInformation("start transfer data");
                var bll = new DataTransferDemo(loggerFactory.CreateLogger<DataTransferDemo>(), new TransferFactory(loggerFactory));
                bll.TransTestTableData(source, _config.GetConnectionString(source), target, _config.GetConnectionString(target));

            }
            catch (Exception ex)
            {
                logger.LogTrace(ex, "Execute Job Exception");
            }
            return Task.CompletedTask;
        }

        private void ResetTable(string targettype,Type tableModel)
        {
            var tablename = tableModel.GetCustomAttribute<TableAttribute>()?.Name;
            if (string.IsNullOrWhiteSpace(tablename)) return;
            var sql = $" truncate table {tablename}";
            if (Enum.TryParse<DatabaseType>(targettype, true, out DatabaseType type))
            {
                switch (type)
                {
                    case DatabaseType.SqlServer:
                        if (_sqlServerDBContext.Demo.Count() > 0) _sqlServerDBContext.Database.ExecuteSqlRaw(sql);
                        break;
                    case DatabaseType.MySQL:
                        if (_mySQLDBContext.Demo.Count() > 0) _mySQLDBContext.Database.ExecuteSqlRaw(sql);
                        break;
                    case DatabaseType.Oracle:
                        if (_oracleDBContext.Demo.Count() > 0) _oracleDBContext.Database.ExecuteSqlRaw(sql);
                        break;
                    default:
                        break;
                }
            }
        }

        protected void LoadTestTableData(string sourcetype,int num)
        {
            var loopnum = num / 100 + (num % 100 > 0 ? 1 : 0);
            for (int i = 0; i < loopnum; i++)
            {
                var insertArray = new List<M_BulkCopyDemo>();
                for (int j = 0; j < 100; j++)
                {
                    M_BulkCopyDemo m = new M_BulkCopyDemo();
                    m.NAME = Guid.NewGuid().ToString("N");
                    m.CREATETICKS = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                    m.CREATETIME = DateTimeOffset.Now.ToString("yyyy-MM-dd HH:mm:ss"); 
                    m.CONTEXT = RandomStr(100);
                    insertArray.Add(m);
                }
                if (Enum.TryParse<DatabaseType>(sourcetype, true, out DatabaseType type))
                {
                    switch (type)
                    {
                        case DatabaseType.SqlServer:
                            _sqlServerDBContext.Demo.AddRange(insertArray.ToArray());
                            _sqlServerDBContext.SaveChanges();
                            break;
                        case DatabaseType.MySQL:
                            _mySQLDBContext.Demo.AddRange(insertArray.ToArray());
                            _mySQLDBContext.SaveChanges();
                            break;
                        case DatabaseType.Oracle:
                            _oracleDBContext.Demo.AddRange(insertArray.ToArray());
                            _oracleDBContext.SaveChanges();
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        private string RandomStr(int length)
        {
            StringBuilder sb = new StringBuilder();
            var str = "abcdefghijklmnopqrstuvwxyz";
            Random random = new Random();
            for (int i = 0; i < length; i++)
            {
                var idx = random.Next(0, 25);
                sb.Append(str[idx]);
                if (sb.Length>=length)
                {
                    break;
                }
            }
            return sb.ToString();
        }
    }
}