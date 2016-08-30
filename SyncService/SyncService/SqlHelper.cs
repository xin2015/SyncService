using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncService
{
    /// <summary>
    /// 数据库工具类
    /// </summary>
    public class SqlHelper
    {
        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public string ConnectionString { get; private set; }
        /// <summary>
        /// 默认实例
        /// </summary>
        public static SqlHelper Default { get; private set; }

        static SqlHelper()
        {
            string connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
            Default = new SqlHelper(connectionString);
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        public SqlHelper(string connectionString)
        {
            ConnectionString = connectionString;
        }

        #region SqlCommand
        public SqlCommand GetInitSqlCommand(SqlConnection conn, CommandType cmdType, string cmdText, params SqlParameter[] cmdParameters)
        {
            SqlCommand cmd = new SqlCommand(cmdText, conn);
            cmd.CommandType = cmdType;
            if (cmdParameters.Any())
            {
                cmd.Parameters.AddRange(cmdParameters);
            }
            return cmd;
        }

        public SqlCommand GetInitSqlCommand(SqlConnection conn, string cmdText, params SqlParameter[] cmdParameters)
        {
            SqlCommand cmd = new SqlCommand(cmdText, conn);
            if (cmdParameters.Any())
            {
                cmd.Parameters.AddRange(cmdParameters);
            }
            return cmd;
        }
        #endregion
        #region ExecuteNonQuery
        public int ExecuteNonQuery(CommandType cmdType, string cmdText, params SqlParameter[] cmdParameters)
        {
            int result;
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                SqlCommand cmd = GetInitSqlCommand(conn, cmdType, cmdText, cmdParameters);
                result = cmd.ExecuteNonQuery();
            }
            return result;
        }

        public int ExecuteNonQuery(string cmdText, params SqlParameter[] cmdParameters)
        {
            int result;
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                SqlCommand cmd = GetInitSqlCommand(conn, cmdText, cmdParameters);
                result = cmd.ExecuteNonQuery();
            }
            return result;
        }
        #endregion
        #region ExecuteScalar
        public object ExecuteScalar(CommandType cmdType, string cmdText, params SqlParameter[] cmdParameters)
        {
            object result;
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                SqlCommand cmd = GetInitSqlCommand(conn, cmdType, cmdText, cmdParameters);
                result = cmd.ExecuteScalar();
            }
            return result;
        }

        public object ExecuteScalar(string cmdText, params SqlParameter[] cmdParameters)
        {
            object result;
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                SqlCommand cmd = GetInitSqlCommand(conn, cmdText, cmdParameters);
                result = cmd.ExecuteScalar();
            }
            return result;
        }
        #endregion
        #region ExecuteDataTable
        public DataTable ExecuteDataTable(CommandType cmdType, string cmdText, params SqlParameter[] cmdParameters)
        {
            DataTable result = new DataTable();
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(GetInitSqlCommand(conn, cmdType, cmdText, cmdParameters));
                adapter.Fill(result);
            }
            return result;
        }

        public DataTable ExecuteDataTable(string cmdText, params SqlParameter[] cmdParameters)
        {
            DataTable result = new DataTable();
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(GetInitSqlCommand(conn, cmdText, cmdParameters));
                adapter.Fill(result);
            }
            return result;
        }
        #endregion
        #region ExecuteList
        public List<T> ExecuteList<T>(CommandType cmdType, string cmdText, params SqlParameter[] cmdParameters) where T : class, new()
        {
            DataTable dt = ExecuteDataTable(cmdType, cmdText, cmdParameters);
            return dt.GetList<T>();
        }

        public List<T> ExecuteList<T>(string cmdText, params SqlParameter[] cmdParameters) where T : class, new()
        {
            DataTable dt = ExecuteDataTable(cmdText, cmdParameters);
            return dt.GetList<T>();
        }
        #endregion
        #region Insert by SqlBulkCopy
        public void Insert(DataTable dt)
        {
            using (SqlBulkCopy sbc = new SqlBulkCopy(ConnectionString))
            {
                sbc.DestinationTableName = dt.TableName;
                sbc.BatchSize = 50000;
                foreach (DataColumn item in dt.Columns)
                {
                    sbc.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                }
                sbc.WriteToServer(dt);
            }
        }
        #endregion
    }
}
