using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncService
{
    public class MissingData
    {
        public string Code { get; set; }
        public DateTime CreateTime { get; set; }
        public int MissTimes { get; set; }
        public bool Status { get; set; }
        public string Exception { get; set; }
        public string MCode { get; set; }
        public string PCode { get; set; }
        public DateTime Time { get; set; }
    }

    public class MissingDataHelper
    {
        public static int MaxMissTimes { get; set; }
        public static string TableName { get; set; }
        static MissingDataHelper()
        {
            MaxMissTimes = 10;
            TableName = "MissingData";
        }

        public static List<MissingData> GetList(string code)
        {
            string cmdText = string.Format("select * from {0} where Code = @Code and Status = 1 and MissTimes <= @MaxMissTimes", TableName);
            SqlParameter[] parameters = new SqlParameter[]
            {
                new SqlParameter("@Code", code),
                new SqlParameter("@MaxMissTimes", MaxMissTimes)
            };
            return SqlHelper.Default.ExecuteList<MissingData>(cmdText, parameters);
        }

        public void InsertMissData(string code, DateTime time, string exception = null, string mCode = null, string pCode = null)
        {
            MissingData data = new MissingData();
            data.Code = code;
            data.CreateTime = DateTime.Now;
            data.Time = DateTime.Today.AddHours(DateTime.Now.Hour);
            data.Exception = exception;
            data.MCode = mCode;
            data.PCode = pCode;
        }

        public static void Update(IEnumerable<MissingData> collection)
        {

        }

        public static void Update(MissingData data)
        {
            string cmdText = string.Format("update {0} set CreateTime = @CreateTime, MissTimes = @MissTimes, Status = @Status, Exception = @Exception where Code = @Code and MCode = @MCode and PCode = @PCode and Time = @Time");
            List<SqlParameter> paramList = new List<SqlParameter>();
            paramList.Add(new SqlParameter("Code", data.Code));
            paramList.Add(new SqlParameter("CreateTime", data.CreateTime));
            paramList.Add(new SqlParameter("Exception", data.Exception));
            paramList.Add(new SqlParameter("MCode", data.MCode));
            paramList.Add(new SqlParameter("MissTimes", data.MissTimes));
            paramList.Add(new SqlParameter("PCode", data.PCode));
            paramList.Add(new SqlParameter("Status", data.Status));
            paramList.Add(new SqlParameter("Time", data.Time));
            SqlHelper.Default.ExecuteNonQuery(cmdText, paramList.ToArray());
        }
    }
}
