using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace SimpleETL.Quartz.Job
{
    public class PushDataToBigDataJob : IJob, IDisposable
    {

        private readonly ILogger<PushDataToBigDataJob> logger;

        public PushDataToBigDataJob(ILogger<PushDataToBigDataJob> logger)
        {
            this.logger = logger;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            logger.LogInformation(context.JobDetail.Key + " job executing, triggered by " + context.Trigger.Key);
            await Task.Delay(TimeSpan.FromSeconds(1));
        }

        public void Dispose()
        {
            logger.LogInformation("Example job disposing");
        }
    }
}
