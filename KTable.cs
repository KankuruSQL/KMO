 using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using smo = Microsoft.SqlServer.Management.Smo;

namespace KMO
{
    /// <summary>
    /// Microsoft.SqlServer.Management.Smo.Table extension class
    /// </summary>
    public static class KTable
    {
        #region Procedure scripting
        /// <summary>
        /// Generates the CREATE PROCEDURE statement to List the whole table
        /// </summary>
        /// <param name="t" value="A SMO table"></param>
        /// <param name="header" value="Your custom header" remarks="Can be empty if you don't need header"></param>
        /// <returns>Return the DDL script</returns>
        public static string ScriptProcedureList(this smo.Table t, string header = "")
        {
            string create = @"CREATE PROCEDURE [{0}].[{1}_List] 
AS ";
            StringBuilder sb = new StringBuilder();
            if (!string.IsNullOrEmpty(header))
            {
                sb.AppendLine(header);
            }
            sb.AppendLine(string.Format(create, t.Schema, t.Name));
            IEnumerable<string> columns = t.Columns.Cast<smo.Column>()
                .Select(c => string.Format("[{0}]", c.Name)).AsEnumerable();
            string column = string.Join("\r\n\t, ", columns);

            sb.AppendLine(string.Format("SELECT {0}", column));
            sb.AppendLine(string.Format("FROM [{0}].[{1}]", t.Schema, t.Name));
            sb.AppendLine("GO");
            return sb.ToString();
        }

        /// <summary>
        /// Generates the CREATE PROCEDURE statement to select 1 line from the PK
        /// </summary>
        /// <param name="t" value="A SMO table"></param>
        /// <param name="header" value="Your custom header" remarks="Can be empty if you don't need header"></param>
        /// <returns>Return the DDL script</returns>
        public static string ScriptProcedureSelect(this smo.Table t, string header = "")
        {
            smo.Server s = t.Parent.Parent;
            s.SetDefaultInitFields(typeof(Microsoft.SqlServer.Management.Smo.Column), true);
            string create = @"CREATE PROCEDURE [{0}].[{1}_Select]
{2}AS
";
            StringBuilder sb = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbWhere = new StringBuilder();
            bool hasPrimary = false;

            IEnumerable<string> columns = t.Columns.Cast<smo.Column>()
                .Select(c => string.Format("[{0}]", c.Name)).AsEnumerable();
            string column = string.Join("\r\n\t, ", columns);

            bool firstPrimary = true;
            foreach (smo.Column c in t.Columns)
            {
                if (c.InPrimaryKey)
                {
                    hasPrimary = true;
                    if (!firstPrimary)
                    {
                        sbParam.Append(", ");
                        sbWhere.Append("\tAND ");
                    }
                    firstPrimary = false;
                    sbParam.AppendLine(string.Format("\t@{0} {1}", c.Name, c.DataType.ScriptToSql()));
                    sbWhere.AppendLine(string.Format("[{0}] = @{1}", c.Name, c.Name.Replace(" ", "_")));
                }
            }
            if (hasPrimary)
            {
                if (!string.IsNullOrEmpty(header))
                {
                    sb.AppendLine(header);
                }
                sb.Append(string.Format(create, t.Schema, t.Name, sbParam.ToString()));
                sb.AppendLine(string.Format("SELECT {0}", column.ToString()));
                sb.AppendLine(string.Format("FROM [{0}].[{1}]", t.Schema, t.Name));
                sb.Append(string.Format("WHERE {0}", sbWhere.ToString()));
                sb.AppendLine("GO");
            }
            else
            {
                sb.AppendLine("-- ####### Scripting error #######");
                sb.AppendLine(string.Format("-- {0}.{1} has no primary key !", t.Schema, t.Name));
                sb.AppendLine("-- ###############################");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Generates the CREATE PROCEDURE statement to delete 1 line from the PK
        /// </summary>
        /// <param name="t" value="A SMO table"></param>
        /// <param name="header" value="Your custom header" remarks="Can be empty if you don't need header"></param>
        /// <returns>Return the DDL script</returns>
        public static string ScriptProcedureDelete(this smo.Table t, string header = "")
        {
            smo.Server s = t.Parent.Parent;
            s.SetDefaultInitFields(typeof(Microsoft.SqlServer.Management.Smo.Column), true);
            string create = @"CREATE PROCEDURE [{0}].[{1}_Delete]
{2}AS";
            StringBuilder sb = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbWhere = new StringBuilder();
            bool hasPrimary = false;
            bool firstPrimary = true;
            foreach (smo.Column c in t.Columns)
            {
                if (c.InPrimaryKey)
                {
                    hasPrimary = true;
                    if (!firstPrimary)
                    {
                        sbParam.Append(", ");
                        sbWhere.Append("\tAND ");
                    }
                    firstPrimary = false;
                    sbParam.AppendLine(string.Format("\t@{0} {1}", c.Name, c.DataType.ScriptToSql()));
                    sbWhere.AppendLine(string.Format("[{0}] = @{1}", c.Name, c.Name.Replace(" ", "_")));
                }
            }
            if (hasPrimary)
            {
                if (!string.IsNullOrEmpty(header))
                {
                    sb.AppendLine(header);
                }
                sb.AppendLine(string.Format(create, t.Schema, t.Name, sbParam.ToString()));
                sb.AppendLine(string.Format("DELETE FROM [{0}].[{1}]", t.Schema, t.Name));
                sb.Append(string.Format("WHERE {0}", sbWhere.ToString()));
                sb.AppendLine("GO");
            }
            else
            {
                sb.AppendLine("-- ####### Scripting error #######");
                sb.AppendLine(string.Format("-- {0}.{1} has no primary key !", t.Schema, t.Name));
                sb.AppendLine("-- ###############################");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Generates the CREATE PROCEDURE statement to update 1 line from the PK
        /// </summary>
        /// <param name="t" value="A SMO table"></param>
        /// <param name="header" value="Your custom header" remarks="Can be empty if you don't need header"></param>
        /// <returns>Return the DDL script</returns>
        public static string ScriptProcedureUpdate(this smo.Table t, string header = "")
        {
            smo.Server s = t.Parent.Parent;
            s.SetDefaultInitFields(typeof(Microsoft.SqlServer.Management.Smo.Column), true);
            string create = @"CREATE PROCEDURE [{0}].[{1}_Update]
{2}AS";
            StringBuilder sb = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbWhere = new StringBuilder();
            StringBuilder sbSet = new StringBuilder();
            bool hasPrimary = false;
            bool hasOtherColumn = false;
            bool firstPrimary = true;
            bool firstSet = true;
            bool firstParam = true;
            foreach (smo.Column c in t.Columns)
            {
                if (c.InPrimaryKey)
                {
                    hasPrimary = true;
                    if (!firstPrimary)
                    {
                        sbWhere.Append("\tAND ");
                    }
                    firstPrimary = false;
                    sbWhere.AppendLine(string.Format("[{0}] = @{1}", c.Name, c.Name.Replace(" ", "_")));
                }
                else
                {
                    if (!c.Identity && !c.Computed) // We can't update identity and computed
                    {
                        hasOtherColumn = true;
                        if (!firstSet)
                        {
                            sbSet.Append(", ");
                        }
                        firstSet = false;
                        sbSet.AppendLine(string.Format("\t[{0}] = @{1}", c.Name, c.Name.Replace(" ", "_"))); 
                    }
                }
                if (!firstParam)
                {
                    sbParam.Append("\t, ");
                }
                firstParam = false;
                sbParam.AppendLine(string.Format("@{0} {1}", c.Name.Replace(" ", "_"), c.DataType.ScriptToSql()));
            }
            if (hasPrimary && hasOtherColumn)
            {
                if (!string.IsNullOrEmpty(header))
                {
                    sb.AppendLine(header);
                }
                sb.AppendLine(string.Format(create, t.Schema, t.Name, sbParam.ToString()));
                sb.Append(string.Format("UPDATE [{0}].[{1}]", t.Schema, t.Name));
                sb.Append(string.Format("SET {0}", sbSet.ToString()));
                sb.Append(string.Format("WHERE {0}", sbWhere.ToString()));
                sb.AppendLine("GO");
            }
            else
            {
                sb.AppendLine("-- ####### Scripting error #######");
                sb.AppendLine(string.Format("-- {0}.{1} has no primary key or no column to update !", t.Schema, t.Name));
                sb.AppendLine("-- ###############################");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Generates the CREATE PROCEDURE statement to insert 1 line
        /// </summary>
        /// <param name="t" value="A SMO table"></param>
        /// <param name="header" value="Your custom header" remarks="Can be empty if you don't need header"></param>
        /// <returns>Return the DDL script</returns>
        public static string ScriptProcedureInsert(this smo.Table t, string header = "")
        {
            smo.Server s = t.Parent.Parent;
            s.SetDefaultInitFields(typeof(Microsoft.SqlServer.Management.Smo.Column), true);
            string create = @"CREATE PROCEDURE [{0}].[{1}_Insert]
{2}AS ";
            StringBuilder sb = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbValues = new StringBuilder();
            bool hasOtherColumn = false;
            bool first = true;
            foreach (smo.Column c in t.Columns)
            {
                if (!c.Identity && !c.Computed) // We can't update identity and computed
                {
                    hasOtherColumn = true;
                    if (!first)
                    {
                        sbParam.Append(", ");
                        sbColumn.Append("\t, ");
                        sbValues.Append("\t, ");
                    }
                    first = false;
                    sbParam.AppendLine(string.Format("\t@{0} {1}", c.Name.Replace(" ", "_"), c.DataType.ScriptToSql()));
                    sbColumn.AppendLine(string.Format("[{0}]", c.Name));
                    sbValues.AppendLine(string.Format("@{0}", c.Name.Replace(" ", "_")));
                }
            }
            if (hasOtherColumn)
            {
                if (!string.IsNullOrEmpty(header))
                {
                    sb.AppendLine(header);
                }
                sb.AppendLine(string.Format(create, t.Schema, t.Name, sbParam.ToString()));
                sb.AppendLine(string.Format("INSERT INTO [{0}].[{1}]", t.Schema, t.Name));
                sb.AppendLine(string.Format("({0})", sbColumn.ToString()));
                sb.AppendLine(string.Format("values({0})", sbValues.ToString()));
                sb.AppendLine("GO");
            }
            else
            {
                sb.AppendLine("-- ####### Scripting error #######");
                sb.AppendLine(string.Format("-- {0}.{1} has no column to insert !", t.Schema, t.Name));
                sb.AppendLine("-- ###############################");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Generates the CREATE PROCEDURE statement to select 1 line from the PK
        /// </summary>
        /// <param name="t" value="A SMO table"></param>
        /// <param name="header" value="Your custom header" remarks="Can be empty if you don't need header"></param>
        /// <returns>Return the DDL script</returns>
        public static string ScriptProcedureSelectWithTVP(this smo.Table t, string header = "")
        {
            smo.Server s = t.Parent.Parent;
            s.SetDefaultInitFields(typeof(Microsoft.SqlServer.Management.Smo.Column), true);
            string udtt = @"
CREATE TYPE [{0}].[{1}_TVP] AS TABLE(
{2})
GO
";
            string create = @"CREATE PROCEDURE [{0}].[{1}_SelectTVP]
{2}AS
";
            StringBuilder sb = new StringBuilder();
            StringBuilder sbColumnTvp = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbWhere = new StringBuilder();
            bool hasPrimary = false;
            bool firstPrimary = true;

            IEnumerable<string> columns = t.Columns.Cast<smo.Column>()
                .Select(c => string.Format("[{0}]", c.Name)).AsEnumerable();
            string column = string.Join("\r\n\t, ", columns);

            foreach (smo.Column c in t.Columns)
            {
                if (c.InPrimaryKey)
                {
                    hasPrimary = true;
                    sbColumnTvp.Append("\t");
                    if (!firstPrimary)
                    {
                        sbWhere.Append("\tAND ");
                        sbColumnTvp.Append(", ");
                    }
                    else
                    {
                        sbParam.AppendLine(string.Format("\t@TVP [{0}].[{1}_TVP] READONLY", t.Schema, t.Name));
                    }
                    firstPrimary = false;
                    sbColumnTvp.AppendLine(string.Format("[{0}] {1}", c.Name, c.DataType.ScriptToSql()));
                    sbWhere.AppendLine(string.Format("a.[{0}] = b.[{0}]", c.Name));
                }
            }
            if (hasPrimary)
            {
                sb.Append(string.Format(udtt, t.Schema, t.Name, sbColumnTvp.ToString()));
                if (!string.IsNullOrEmpty(header))
                {
                    sb.AppendLine(header);
                }
                sb.Append(string.Format(create, t.Schema, t.Name, sbParam.ToString()));
                sb.AppendLine(string.Format("SELECT {0}", column));
                sb.AppendLine(string.Format("FROM [{0}].[{1}] a", t.Schema, t.Name));
                sb.AppendLine(string.Format("WHERE EXISTS (SELECT * FROM @TVP b WHERE {0})", sbWhere.ToString()));
                sb.AppendLine("GO");
            }
            else
            {
                sb.AppendLine("-- ####### Scripting error #######");
                sb.AppendLine(string.Format("-- {0}.{1} has no primary key !", t.Schema, t.Name));
                sb.AppendLine("-- ###############################");
            }
            return sb.ToString();
        }
        #endregion

        #region Compare Data
        /// <summary>
        /// Compare 2 tables data.
        /// Checksum doesn't manage these types : Xml, Image, Geography, ntext, text
        /// </summary>
        /// <param name="t1">First smo.Table</param>
        /// <param name="t2">Second smo.Table</param>
        /// <returns>Return true if the 2 tables have the same checksum</returns>
        public static bool CompareDataLight(this smo.Table t1, smo.Table t2)
        {
            return (t1.DataChecksum() == t2.DataChecksum());
        }

        /// <summary>
        /// Get the checksum for a table
        /// Checksum doesn't manage these types : Xml, Image, Geography, ntext, text
        /// </summary>
        /// <param name="t">a smo.table</param>
        /// <returns>Int64 represents Checksum_agg(Checksum(column list))</returns>
        public static Int64 DataChecksum(this smo.Table t)
        {
            string column = GetJoinedColumns(t);
            string query = string.Format("select checksum_agg(CHECKSUM({0})) as nb from [{1}].[{2}]", column, t.Schema, t.Name);
            smo.Database d = t.Parent;
            DataSet ds = d.ExecuteWithResults(query);
            Int64 i;
            Int64.TryParse(ds.Tables[0].Rows[0]["nb"].ToString(), out i);
            return i;
        }

        /// <summary>
        /// Compare 2 tables data. Less performant than CompareDataLight but in case of difference
        /// you're able to know which rows are differents
        /// </summary>
        /// <param name="t1">smo.table 1</param>
        /// <param name="t2">smo.table 2</param>
        /// <param name="dtRowsAdded">this DataTable contains rows from table1 not existing in table2</param>
        /// <param name="dtRowsDeleted">this DataTable contains rows from table2 not existing in table1</param>
        /// <param name="dtRowsUpdated">this DataTable contains rows existing in the 2 tables but different</param>
        /// <param name="details">if false, DataTables will contains only PK columns + checksum. If true, Datatables will contains every columns</param>
        /// <returns></returns>
        public static bool CompareData(this smo.Table t1, smo.Table t2, out DataTable dtRowsAdded, out DataTable dtRowsDeleted, out DataTable dtRowsUpdated, bool details = false)
        {
            bool hasSameData = true;
            dtRowsAdded = null;
            dtRowsDeleted = null;
            dtRowsUpdated = null;

            string columnPK1 = GetJoinedColumnsPKOnly(t1);
            string columnPK2 = GetJoinedColumnsPKOnly(t2);
            string column1 = GetJoinedColumnsWithoutPK(t1);
            string column2 = GetJoinedColumnsWithoutPK(t2);

            string queryTemplate = "SELECT {0} as pk, CHECKSUM({1}) as rowchecksum FROM [{2}].[{3}]";
            if (details)
            {
                queryTemplate = "SELECT {0} as pk, CHECKSUM({1}) as rowchecksum, * FROM [{2}].[{3}]";
            }
            string query1 = string.Format(queryTemplate, columnPK1, column1, t1.Schema, t1.Name);
            string query2 = string.Format(queryTemplate, columnPK2, column2, t2.Schema, t2.Name);

            smo.Database d1 = t1.Parent;
            smo.Database d2 = t2.Parent;
            EnumerableRowCollection<DataRow> r1 = d1.ExecuteWithResults(query1).Tables[0].AsEnumerable();
            EnumerableRowCollection<DataRow> r2 = d2.ExecuteWithResults(query2).Tables[0].AsEnumerable();

            bool hasRowsAdded = CompareRowsAdded(r1, r2, out dtRowsAdded);
            bool hasRowsDeleted = CompareRowsAdded(r2, r1, out dtRowsDeleted);
            bool hasRowsUpdated = CompareRowsUpdated(r2, r1, out dtRowsUpdated);

            if (hasRowsAdded || hasRowsDeleted || hasRowsUpdated)
            {
                hasSameData = false;
            }
            return hasSameData;
        }

        /// <summary>
        /// concat PK columns from a table
        /// </summary>
        /// <param name="t">smo.table</param>
        /// <returns>column list</returns>
        private static string GetJoinedColumnsPKOnly(smo.Table t)
        {
            IEnumerable<string> columnsPK = t.Columns.Cast<smo.Column>()
                .Where(c => c.InPrimaryKey)
                .Select(c => string.Format(" cast({0} as NVARCHAR)", c.Name)).AsEnumerable();
            return string.Join(" + '|' + ", columnsPK);
        }

        /// <summary>
        /// concat columns from a table without PK and unsupported datatypes
        /// </summary>
        /// <param name="t">smo.table</param>
        /// <returns>column list</returns>
        private static string GetJoinedColumnsWithoutPK(smo.Table t)
        {
            StringBuilder sb = new StringBuilder();
            IEnumerable<smo.Column> columns = t.Columns.Cast<smo.Column>()
                .Where(c => !c.InPrimaryKey)
                .Where(c => c.DataType.SqlDataType.ToString() != "Xml"
                        && c.DataType.SqlDataType.ToString() != "VarChar"
                        && c.DataType.SqlDataType.ToString() != "NVarChar"
                        && c.DataType.SqlDataType.ToString() != "SysName"
                        && c.DataType.SqlDataType.ToString() != "Char"
                        && c.DataType.SqlDataType.ToString() != "NChar"
                        && c.DataType.SqlDataType.ToString() != "Image"
                        && c.DataType.SqlDataType.ToString() != "Geography"
                        && c.DataType.SqlDataType.ToString() != "ntext"
                        && c.DataType.SqlDataType.ToString() != "text");
            string s1 = string.Join(", ", columns);
            if (!string.IsNullOrEmpty(s1))
            {
                sb.Append(s1);
            }
            IEnumerable<smo.Column> columnsString = t.Columns.Cast<smo.Column>()
                .Where(c => !c.InPrimaryKey)
                .Where(c => c.DataType.SqlDataType.ToString() == "VarChar"
                        || c.DataType.SqlDataType.ToString() == "NVarChar"
                        || c.DataType.SqlDataType.ToString() == "SysName"
                        || c.DataType.SqlDataType.ToString() == "Char"
                        || c.DataType.SqlDataType.ToString() == "NChar");
            string s2 = "HASHBYTES('MD5', " + string.Join(") , HASHBYTES('MD5', ", columnsString) + ")";
            if (!string.IsNullOrEmpty(s2))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(s2);
            }
            return sb.ToString();
        }


        /// <summary>
        /// concat all columns from a table without unsupported datatypes
        /// </summary>
        /// <param name="t">smo.table</param>
        /// <returns>column list</returns>
        private static string GetJoinedColumns(smo.Table t)
        {
            StringBuilder sb = new StringBuilder();
            IEnumerable<smo.Column> columns = t.Columns.Cast<smo.Column>()
                .Where(c => c.DataType.SqlDataType.ToString() != "Xml"
                        && c.DataType.SqlDataType.ToString() != "VarChar"
                        && c.DataType.SqlDataType.ToString() != "NVarChar"
                        && c.DataType.SqlDataType.ToString() != "SysName"
                        && c.DataType.SqlDataType.ToString() != "Char"
                        && c.DataType.SqlDataType.ToString() != "NChar"
                        && c.DataType.SqlDataType.ToString() != "Image"
                        && c.DataType.SqlDataType.ToString() != "Geography"
                        && c.DataType.SqlDataType.ToString() != "ntext"
                        && c.DataType.SqlDataType.ToString() != "text");
            string s1 = string.Join(", ", columns);
            if (!string.IsNullOrEmpty(s1))
            {
                sb.Append(s1);
            }
            IEnumerable<smo.Column> columnsString = t.Columns.Cast<smo.Column>()
                .Where(c => c.DataType.SqlDataType.ToString() == "VarChar"
                        || c.DataType.SqlDataType.ToString() == "NVarChar"
                        || c.DataType.SqlDataType.ToString() == "SysName"
                        || c.DataType.SqlDataType.ToString() == "Char"
                        || c.DataType.SqlDataType.ToString() == "NChar");
            string s2 = "HASHBYTES('MD5', " + string.Join(") , HASHBYTES('MD5', ", columnsString) + ")";
            if (!string.IsNullOrEmpty(s2))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(s2);
            }
            return sb.ToString();
        }

        /// <summary>
        /// To look for differences between 2 DataTables. Determine which rows are present in one DataTable
        /// and not in the second
        /// </summary>
        /// <param name="r1">the first DataTable casted as AsEnumerable()</param>
        /// <param name="r2">the second DataTable casted as AsEnumerable()</param>
        /// <param name="dtRowsAdded">This DataTable contains the result</param>
        /// <returns>Bool. If true differences found.</returns>
        private static bool CompareRowsAdded(EnumerableRowCollection<DataRow> r1, EnumerableRowCollection<DataRow> r2, out DataTable dtRowsAdded)
        {
            bool hasRowsAdded = false;
            var added = r1.Select(r => r.Field<string>("pk"))
                .Except(r2.Select(r => r.Field<string>("pk")));
            var va = (from row in r1.AsEnumerable()
                      join pk in added
                      on row.Field<string>("pk") equals pk
                      select row);
            if (va.Count() > 0)
            {
                dtRowsAdded = va.CopyToDataTable();
                hasRowsAdded = true;
            }
            else
            {
                dtRowsAdded = new DataTable();
            }
            return hasRowsAdded;
        }

        /// <summary>
        /// To look for differences between 2 DataTables. Determine which rows are present in each DataTable
        /// but have not the same checksum.
        /// </summary>
        /// <param name="r1">the first DataTable casted as AsEnumerable()</param>
        /// <param name="r2">the second DataTable casted as AsEnumerable()</param>
        /// <param name="dtRowsAdded">This DataTable contains the result</param>
        /// <returns>Bool. If true differences found.</returns>
        private static bool CompareRowsUpdated(EnumerableRowCollection<DataRow> r1, EnumerableRowCollection<DataRow> r2, out DataTable dtRowsUpdated)
        {
            bool hasRowsUpdated = false;
            var vu = (from row1 in r1
                      join row2 in r2
                      on row1.Field<string>("pk") equals row2.Field<string>("pk")
                      where row1.Field<int>("rowchecksum") != row2.Field<int>("rowchecksum")
                      select row1);
            if (vu.Count() > 0)
            {
                dtRowsUpdated = vu.CopyToDataTable();
                hasRowsUpdated = true;
            }
            else
            {
                dtRowsUpdated = new DataTable();
            }
            return hasRowsUpdated;
        }

        #endregion

    }
}
