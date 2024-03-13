using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using SimpleETL.Business;
using SimpleETL.ConsoleHost.Database;
using SimpleETL.DB.Common;
using SimpleETL.DB.Trans;
using SimpleETL.DB.Trans.Interface;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Text;

namespace SimpleETL.ConsoleHost.Jobs
{
    [DisallowConcurrentExecution]
    public class DataTransDemoJob : IJob
    {
        public DataTransDemoJob(ILoggerFactory logger, IConfiguration config, ITransferFactory transferFactory)
        {
            this.logger = logger.CreateLogger<DataTransDemoJob>();
            this.config = config;
            this.transferFactory = transferFactory;
        }
        private readonly IConfiguration config;
        private readonly ILogger logger;
        private readonly ITransferFactory transferFactory;
        public Task Execute(IJobExecutionContext context)
        {
            logger.LogInformation("Job Start");
            try
            {
                var sourcetype = context.JobDetail.JobDataMap.Get("sourcetype") as string;
                var targettype = context.JobDetail.JobDataMap.Get("targettype") as string;
                if (string.IsNullOrEmpty(sourcetype) || string.IsNullOrWhiteSpace(targettype))
                {
                    logger.LogWarning("目标库或源数据库不明确！");
                    return Task.CompletedTask;
                }
                var sourceconnection = config[sourcetype];
                var targetconnection = config[targettype];

                logger.LogInformation("load schema");

                LoadTestTableData(sourcetype, sourceconnection, 10000);

                logger.LogInformation("reset target table");
                ResetTable(targettype, targetconnection, typeof(M_BulkCopyDemo));

                logger.LogInformation("start transfer data");
                var bll = new DataTransferDemo(logger, new TransferFactory(logger), config, sourcetype, targettype);
                bll.TransTestTableData(sourcetype, sourceconnection, targettype, targetconnection);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Execute Job Exception");
            }
            return Task.CompletedTask;
        }

        private void ResetTable(string targettype, string targetconnection, Type tableModel)
        {
            var tablename = tableModel.GetCustomAttribute<TableAttribute>()?.Name;
            if (string.IsNullOrWhiteSpace(tablename)) return;
            var sql = $" truncate table {tablename}";
            if (Enum.TryParse<DatabaseType>(targettype, true, out DatabaseType type))
            {
                switch (type)
                {
                    case DatabaseType.SqlServer:
                        using (var context = new SqlServerDBContext(targetconnection))
                        {
                            context.Database.EnsureCreated();
                            if (context.Demo.Count() > 0)
                                context.Database.ExecuteSqlRaw(sql);
                        }
                        break;
                    case DatabaseType.MySQL:
                        using (var context = new MySQLDBContext(targetconnection))
                        {
                            context.Database.EnsureCreated();
                            if (context.Demo.Count() > 0)
                                context.Database.ExecuteSqlRaw(sql);
                        }
                        break;
                    case DatabaseType.Oracle:
                        using (var context = new OracleDBContext(targetconnection))
                        {
                            context.Database.EnsureCreated();
                            if (context.Demo.Count() > 0)
                                context.Database.ExecuteSqlRaw(sql);
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        protected void LoadTestTableData(string sourcetype, string sourceconnection, int num)
        {
            if (Enum.TryParse<DatabaseType>(sourcetype, true, out DatabaseType type))
            {
                switch (type)
                {
                    case DatabaseType.SqlServer:
                        using (var context = new SqlServerDBContext(sourceconnection))
                        {
                            context.Database.EnsureCreated();
                            if (context.Demo.Count() <= 0)
                            {
                                logger.LogInformation($"load test data :{sourcetype}");
                                var loopnum = num / 100 + (num % 100 > 0 ? 1 : 0);
                                for (int i = 0; i < loopnum; i++)
                                {
                                    context.Demo.AddRange(CreateTestData(100).ToArray());
                                    context.SaveChanges();
                                }
                            }
                        }
                        break;
                    case DatabaseType.MySQL:
                        using (var context = new MySQLDBContext(sourceconnection))
                        {
                            context.Database.EnsureCreated();
                            if (context.Demo.Count() <= 0)
                            {
                                logger.LogInformation($"load test data :{sourcetype}");
                                var loopnum = num / 100 + (num % 100 > 0 ? 1 : 0);
                                for (int i = 0; i < loopnum; i++)
                                {
                                    context.Demo.AddRange(CreateTestData(100).ToArray());
                                    context.SaveChanges();
                                }
                            }
                        }
                        break;
                    case DatabaseType.Oracle:
                        using (var context = new OracleDBContext(sourceconnection))
                        {
                            context.Database.EnsureCreated();
                            if (context.Demo.Count() <= 0)
                            {
                                logger.LogInformation($"load test data :{sourcetype}");
                                var loopnum = num / 100 + (num % 100 > 0 ? 1 : 0);
                                for (int i = 0; i < loopnum; i++)
                                {
                                    context.Demo.AddRange(CreateTestData(100).ToArray());
                                    context.SaveChanges();
                                }
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        protected List<M_BulkCopyDemo> CreateTestData(int num = 100)
        {
            var insertArray = new List<M_BulkCopyDemo>();
            for (int j = 0; j < num; j++)
            {
                M_BulkCopyDemo m = new M_BulkCopyDemo();
                m.NAME = Guid.NewGuid().ToString("N");
                m.CREATETICKS = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                m.CREATETIME = DateTimeOffset.Now.ToString("yyyy-MM-dd HH:mm:ss");
                m.CONTEXT = RandomStr(100);
                insertArray.Add(m);
            }
            return insertArray;
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
                if (sb.Length >= length)
                {
                    break;
                }
            }
            return sb.ToString();
        }
    }
}