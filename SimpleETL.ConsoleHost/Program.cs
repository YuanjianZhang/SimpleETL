using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;
using SimpleETL.ConsoleHost.Jobs;
using SimpleETL.DB.Encrypt.Extension;
using SimpleETL.DB.Trans;
using SimpleETL.DB.Trans.Interface;
using SimpleETL.Util;

namespace SimpleETL.ConsoleHost
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            ILogger logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger("Program");
            #region start app
            try
            {
                var separator = new string('-', 30);
                logger.LogInformation($"{separator} Starting host {separator} ");
                var builder = Host.CreateApplicationBuilder(args);
                builder.Configuration
                    .AddEncryptedProvider()
                    .AddJsonFile("appsettings.json")
                    .AddJsonFile($"appsettings.Development.json", true, false)
                    .Build();

                GlobalConfig.Configure = builder.Configuration;

                var env = builder.Environment.IsDevelopment();

                builder.Services.AddLogging(loggerbuilder =>
                {
                    loggerbuilder.ClearProviders();
                    loggerbuilder.AddConsole().AddSimpleConsole();
                })
                .AddQuartz((options) =>
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
                                configure.StartAt(DateTime.Now.AddSeconds(5));
                            }
                            , jobconfigure =>
                            {
                                jobconfigure.WithIdentity("testjob");
                                var jobdata = new Dictionary<string, string>
                                {
                                        { "sourcetype", builder.Configuration.GetSection("Task").GetChildren().First().GetValue<string>("source") },
                                        { "targettype", builder.Configuration.GetSection("Task").GetChildren().First().GetValue<string>("target") }
                                };
                                jobconfigure.SetJobData(new(jobdata));
                            });
                    })
                .AddQuartzHostedService(configure =>
                    {
                        configure.AwaitApplicationStarted = true;
                        configure.WaitForJobsToComplete = true;
                    })
                .AddSingleton<ITransferFactory>(serviceProvider =>
                {
                    var logger = serviceProvider.GetRequiredService<ILogger<TransferFactory>>();
                    return new TransferFactory(logger);
                });
                var app = builder.Build();
                await app.RunAsync();

                logger.LogInformation($"{separator} Exit host {separator} ");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Host terminated unexpectedly");
            }
            #endregion
        }
    }
}