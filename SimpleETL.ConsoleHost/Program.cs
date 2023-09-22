using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Quartz;
using Quartz.Util;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using SimpleETL.ConsoleHost.Database;
using SimpleETL.ConsoleHost.Jobs;
using SimpleETL.DB.Trans;
using SimpleETL.DB.Trans.Interface;

namespace SimpleETL.ConsoleHost
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                        .WriteTo.Console(theme: AnsiConsoleTheme.Sixteen)
                        .Enrich.FromLogContext()
                        .MinimumLevel.Debug().CreateLogger();

            try
            {
                var builder = Host.CreateDefaultBuilder(args);
                builder.ConfigureAppConfiguration(builder =>
                    {
                        var configure = new ConfigurationBuilder()
                            .AddJsonFile("appsettings.json")
                            .AddJsonFile($"appsettings.Development.json", true, false)
                            .AddEnvironmentVariables()
                            .Build();
                        builder.Sources.Clear();
                        builder.AddConfiguration(configure);
                    })
                    .ConfigureServices((context, ser) =>
                    {
                        ser.AddQuartz((options) =>
                        {
                            options.ScheduleJob<DataTransDemoJob>(
                                configure =>
                                {
                                    configure.WithIdentity("testtrigger");
                                    configure.ForJob(new JobKey("testjob"));
                                    configure.WithSimpleSchedule(p =>
                                    {
                                        p.WithIntervalInSeconds(30).WithRepeatCount(0).Build();
                                    });
                                    configure.StartNow();
                                }
                                , jobconfigure =>
                                {
                                    jobconfigure.WithIdentity("testjob");
                                    var jobdata = new Dictionary<string, string>
                                    {
                                        { "source", context.Configuration.GetSection("Task").GetChildren().First().GetValue<string>("source") },
                                        { "target", context.Configuration.GetSection("Task").GetChildren().First().GetValue<string>("target") }
                                    };
                                    jobconfigure.SetJobData(new(jobdata));
                                });
                        });

                        ser.AddQuartzHostedService(configure =>
                        {
                            configure.WaitForJobsToComplete = true;
                        });

                        ser.AddSingleton<ITransferFactory, TransferFactory>();
                        if (!string.IsNullOrWhiteSpace(context.Configuration.GetConnectionString("oracle")))
                        {
                            ser.AddDbContext<OracleDBContext>(options =>
                            {
                                options.UseOracle(context.Configuration.GetConnectionString("oracle"));
                            });
                        }
                        if (!string.IsNullOrWhiteSpace(context.Configuration.GetConnectionString("sqlserver")))
                        {
                            ser.AddDbContext<SqlServerDBContext>(options =>
                            {
                                options.UseSqlServer(context.Configuration.GetConnectionString("sqlserver"));
                            });
                        }
                        if (!string.IsNullOrWhiteSpace(context.Configuration.GetConnectionString("mysql")))
                        {
                            ser.AddDbContext<MySQLDBContext>(options =>
                            {
                                options.UseMySql(context.Configuration.GetConnectionString("mysql"), new MySqlServerVersion(new Version(8, 0, 31)));
                            });
                        }

                    }).ConfigureLogging(log =>
                    {
                        log.AddSerilog(Log.Logger);
                    });

                var host = builder.Build();
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                Log.Logger.Fatal(ex, "Server Run Exception!");
            }

        }

    }
}