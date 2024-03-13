using Microsoft.Extensions.Configuration;
using Quartz;

namespace SimpleETL.ConsoleHost.Extension
{
    public static class ServiceCollectionQuartzConfiguratorExtensions
    {
        private static readonly string sectionname = "QuartzJob";
        public static void AddJobAndTrigger<T>(this IServiceCollectionQuartzConfigurator quartz, IConfiguration config) where T : IJob
        {
            var JobsConfig = config.GetSection(sectionname);

            string groupName = typeof(T).Name;

            var configKey = $"jobName";

            foreach (var idx in JobsConfig.GetChildren())
            {
                var jobinfo = JobsConfig.GetSection(idx.Key);
                if (jobinfo["enable"].ToUpper() == "TRUE" && jobinfo["jobgroup"] == groupName)
                {
                    var jobname = jobinfo["jobname"];
                    var jobgroup = jobinfo["jobgroup"];
                    var description = jobinfo["description"];
                    var triggersetion = jobinfo.GetSection("trigger");

                    bool startnow = triggersetion["startnow"].ToUpper() == "TRUE";
                    var cronSchedule = triggersetion["cronschedule:cron"];
                    var priority = int.Parse(triggersetion["priority"]);

                    if (string.IsNullOrEmpty(cronSchedule))
                    {
                        throw new Exception($"No Quartz.NET Cron schedule found for job in configuration at {configKey}");
                    }
                    var jobdatasection = jobinfo.GetSection("param").GetChildren();
                    Dictionary<string, string> jobdata = new Dictionary<string, string>();
                    foreach (var item in jobdatasection)
                    {
                        var t = item.GetChildren();

                        if (t != null && t.Count() > 0)
                        {
                            jobdata.Add(item.Key, string.Join(',', t.Select(p => p.Value).ToArray()));
                        }
                        else
                        {
                            jobdata.Add(item.Key, item.Value);
                        }
                    }
                    JobKey key = new JobKey(jobname, jobgroup);
                    quartz.AddJob<T>(opts =>
                    {
                        opts.WithIdentity(key)
                        .WithDescription(description)
                        .SetJobData(new JobDataMap(jobdata));
                    });

                    quartz.AddTrigger(opts =>
                    {
                        opts
                        .ForJob(key)
                        .WithIdentity("trigger_" + jobname, jobgroup)
                        .WithCronSchedule(cronSchedule)
                        .WithPriority(priority);
                        if (startnow) opts.StartNow();
                    });

                }
            }

        }
    }
}
