using Common.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SyncService
{
    /// <summary>
    /// 数据库表与实体类转换类
    /// </summary>
    public static class DataTableHelper
    {
        private static ILog logger;

        static DataTableHelper()
        {
            logger = LogManager.GetLogger("DataTableHelper");
        }

        /// <summary>
        /// 将DataTable转换为实体类集合
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="dt">DataTable</param>
        /// <returns>实体类集合</returns>
        public static List<T> GetList<T>(this DataTable dt) where T : class, new()
        {
            PropertyInfo[] properties = typeof(T).GetProperties().Where(t => dt.Columns.Contains(t.Name)).ToArray();
            int count = dt.Rows.Count;
            T[] array = new T[count];
            for (int i = 0; i < count; i++)
            {
                array[i] = new T();
            }
            foreach (PropertyInfo property in properties)
            {
                Type propertyType = property.PropertyType;
                DataColumn dc = dt.Columns[dt.Columns.IndexOf(property.Name)];
                int i = 0;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                {
                    Type underlyingType = Nullable.GetUnderlyingType(property.PropertyType);
                    if (dc.DataType.Equals(underlyingType))
                    {
                        foreach (DataRow dr in dt.Rows)
                        {
                            if (!Convert.IsDBNull(dr[property.Name]))
                            {
                                property.SetValue(array[i], dr[property.Name]);
                            }
                            i++;
                        }
                    }
                    else
                    {
                        try
                        {
                            foreach (DataRow dr in dt.Rows)
                            {
                                if (!Convert.IsDBNull(dr[property.Name]))
                                {
                                    property.SetValue(array[i], Convert.ChangeType(dr[property.Name], underlyingType));
                                }
                                i++;
                            }
                        }
                        catch (Exception e)
                        {
                            logger.Warn("GetList Convert UnderlyingType Error!", e);
                        }
                    }
                }
                else
                {
                    if (dc.DataType.Equals(property.PropertyType))
                    {
                        foreach (DataRow dr in dt.Rows)
                        {
                            if (!Convert.IsDBNull(dr[property.Name]))
                            {
                                property.SetValue(array[i], dr[property.Name]);
                            }
                            i++;
                        }
                    }
                    else
                    {
                        try
                        {
                            foreach (DataRow dr in dt.Rows)
                            {
                                if (!Convert.IsDBNull(dr[property.Name]))
                                {
                                    property.SetValue(array[i], Convert.ChangeType(dr[property.Name], property.PropertyType));
                                }
                                i++;
                            }
                        }
                        catch (Exception e)
                        {
                            logger.Warn("GetList Convert Error!", e);
                        }
                    }
                }
            }
            return array.ToList();
        }

        /// <summary>
        /// 将实体类集合转换为DataTable
        /// </summary>
        /// <typeparam name="T">实体类</typeparam>
        /// <param name="collection">实体类集合</param>
        /// <param name="tableName">表名</param>
        /// <param name="preclusiveColumnNames">排除的列名</param>
        /// <returns>DataTable</returns>
        public static DataTable GetDataTable<T>(this IEnumerable<T> collection, string tableName, params string[] preclusiveColumnNames)
        {
            DataTable dt = new DataTable();
            dt.TableName = tableName;
            PropertyInfo[] properties = typeof(T).GetProperties();
            if (preclusiveColumnNames.Any())
            {
                properties = properties.Where(o => !preclusiveColumnNames.Contains(o.Name)).ToArray();
            }
            foreach (PropertyInfo property in properties)
            {
                Type propertyType = property.PropertyType;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                {
                    dt.Columns.Add(property.Name, Nullable.GetUnderlyingType(propertyType));
                }
                else
                {
                    dt.Columns.Add(property.Name, propertyType);
                }
            }
            foreach (T data in collection)
            {
                DataRow dr = dt.NewRow();
                foreach (PropertyInfo property in properties)
                {
                    dr[property.Name] = property.GetValue(data) ?? DBNull.Value;
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }
    }
}
