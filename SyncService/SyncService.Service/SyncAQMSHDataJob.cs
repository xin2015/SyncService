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
        public static string LiveTableName { get; set; }
        public static string HistoryTableName { get; set; }
        public static string Code { get; set; }
        static SyncAQMSHDataJob()
        {
            logger = LogManager.GetLogger<SyncAQMSHDataJob>();
            CronExpression = Configuration.SyncAQMSHDataJobCronExpression;
            LiveTableName = "AQIDataPublishLive";
            HistoryTableName = "AQIDataPublishHistory";
        }

        public void Execute(IJobExecutionContext context)
        {
            try
            {
                object state = SqlHelper.Default.ExecuteScalar(string.Format("select max(TimePoint) from {0}", LiveTableName));
                DateTime lastTime = Convert.IsDBNull(state) ? DateTime.MinValue : Convert.ToDateTime(state);
                List<AQIDataPublishLive> list = new List<AQIDataPublishLive>();
                using (SyncNationalAQIPublishDataServiceClient client = new SyncNationalAQIPublishDataServiceClient())
                {
                    list = client.GetAQIDataPublishLive().ToList();
                }
                if (list.Any() && list.First().TimePoint > lastTime)
                {
                    SqlHelper.Default.ExecuteNonQuery(string.Format("delete {0}", LiveTableName));
                    SqlHelper.Default.Insert(list.GetDataTable<AQIDataPublishLive>(LiveTableName));
                    SqlHelper.Default.Insert(list.GetDataTable<AQIDataPublishLive>(HistoryTableName));
                }
                else
                {

                }
            }
            catch (Exception e)
            {

                logger.Error("SyncAQMSHData failed.", e);
            }
        }

        
    }
}
