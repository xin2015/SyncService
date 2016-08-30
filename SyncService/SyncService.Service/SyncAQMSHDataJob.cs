using Common.Logging;
using Quartz;
using SyncService.Service.SyncNationalAQIPublishDataService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncService.Service
{
    public class SyncAQMSHDataJob : IJob, IRecover
    {
        private static ILog logger;
        public static string CronExpression { get; set; }
        public static string RecoverCronExpression { get; set; }
        public static string IsRecoverKey { get; set; }
        public static string LiveTableName { get; set; }
        public static string HistoryTableName { get; set; }

        static SyncAQMSHDataJob()
        {
            logger = LogManager.GetLogger<SyncAQMSHDataJob>();
            CronExpression = Configuration.SyncAQMSHDataJobCronExpression;
            RecoverCronExpression = Configuration.SyncAQMSHDataJobRecoverCronExpression;
            IsRecoverKey = "IsRecoverKey";
            LiveTableName = "AQIDataPublishLive";
            HistoryTableName = "AQIDataPublishHistory";
        }

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                JobDataMap jobDataMap = context.JobDetail.JobDataMap;
                bool isRecover = jobDataMap.GetBooleanValue(IsRecoverKey);
                if (isRecover)
                {

                }
                List<AQIDataPublishLive> list = new List<AQIDataPublishLive>();
                using (SyncNationalAQIPublishDataServiceClient client = new SyncNationalAQIPublishDataServiceClient())
                {
                    list = client.GetAQIDataPublishLive().ToList();
                }
                SqlHelper.Default.Insert(list.GetDataTable<AQIDataPublishLive>(TableName));
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                if (!e.Message.Contains("插入重复键")) logger.Error("SyncAQMSHData failed.", e);
            }
            catch (Exception e)
            {
                logger.Error("SyncAQMSHData failed.", e);
            }
        }
    }
}
