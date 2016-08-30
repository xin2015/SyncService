using Common.Logging;
using Quartz;
using Quartz.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Topshelf;

namespace SyncService.Service
{
    public class SyncDataService : ServiceControl
    {
        private ILog logger;
        private ISchedulerFactory schedulerFactory;
        private IScheduler scheduler;

        public SyncDataService()
        {
            logger = LogManager.GetLogger<SyncDataService>();
            Initialize();
        }

        public virtual void Initialize()
        {
            try
            {
                logger.Info("-------- Initialization Start --------");
                schedulerFactory = new StdSchedulerFactory();
                scheduler = schedulerFactory.GetScheduler();
                logger.Info("-------- Scheduling Jobs --------");
                Assembly asm = Assembly.GetExecutingAssembly();
                Type[] types = asm.GetTypes();
                List<Type> typeList = new List<Type>();
                foreach (Type t in types)
                {
                    if (t.GetInterfaces().Contains(typeof(IJob)))
                    {
                        typeList.Add(t);
                    }
                }
                typeList.ForEach(o =>
                {
                    IJobDetail job = JobBuilder.Create(o)
                        .WithIdentity(string.Format("job{0},", o.Name), "DefaultGroup")
                        .Build();
                    ICronTrigger trigger = (ICronTrigger)TriggerBuilder.Create()
                        .WithIdentity(string.Format("trigger{0}", o.Name), "DefaultGroup")
                        .WithCronSchedule(o.GetProperty("CronExpression").GetValue(null, null).ToString())
                        .Build();
                    scheduler.ScheduleJob(job, trigger);
                });
                logger.Info("-------- Initialization Complete --------");
            }
            catch (Exception e)
            {
                logger.Fatal("Server initialization failed.", e);
            }
        }

        public virtual void Start()
        {
            try
            {
                scheduler.Start();
            }
            catch (Exception ex)
            {
                logger.Fatal("Scheduler start failed.", ex);
            }
        }

        public virtual void Stop()
        {
            try
            {
                scheduler.Shutdown(true);
            }
            catch (Exception ex)
            {
                logger.Error("Scheduler stop failed.", ex);
            }
        }

        public bool Start(HostControl hostControl)
        {
            Start();
            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            Stop();
            return true;
        }
    }
}
