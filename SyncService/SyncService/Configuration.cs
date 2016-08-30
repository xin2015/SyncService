using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncService
{
    public class Configuration
    {
        public static string Description { get; set; }
        public static string DisplayName { get; set; }
        public static string ServiceName { get; set; }
        public static string DefaultCronExpression { get; set; }
        public static string SyncAQMSHDataJobCronExpression { get; set; }
        public static string SyncAQMSHDataJobRecoverCronExpression { get; set; }
        static Configuration()
        {
            Description = ConfigurationManager.AppSettings["Description"];
            DisplayName = ConfigurationManager.AppSettings["DisplayName"];
            ServiceName = ConfigurationManager.AppSettings["ServiceName"];
            DefaultCronExpression = "0 0 0/1 * * ?";
            SyncAQMSHDataJobCronExpression = ConfigurationManager.AppSettings["SyncAQMSHDataJobCronExpression"];
            SyncAQMSHDataJobRecoverCronExpression = ConfigurationManager.AppSettings["SyncAQMSHDataJobRecoverCronExpression"];

            string defaultService = "MyService";
            Description = string.IsNullOrWhiteSpace(Description) ? defaultService : Description;
            DisplayName = string.IsNullOrWhiteSpace(DisplayName) ? defaultService : DisplayName;
            ServiceName = string.IsNullOrWhiteSpace(ServiceName) ? defaultService : ServiceName;
        }
    }
}
