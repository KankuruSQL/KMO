using System;
using System.Collections.Generic;
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
            StringBuilder sbColumn = new StringBuilder();
            bool first = true;
            foreach (smo.Column c in t.Columns)
            {
                if (!first)
                {
                    sbColumn.Append("\t, ");
                }
                first = false;
                sbColumn.AppendLine(string.Format("[{0}]", c.Name));
            }
            sb.Append(string.Format("SELECT {0}", sbColumn.ToString()));
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
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbWhere = new StringBuilder();
            bool hasPrimary = false;
            bool firstCol = true;
            bool firstPrimary = true;
            foreach (smo.Column c in t.Columns)
            {
                if (!firstCol)
                {
                    sbColumn.Append("\t, ");
                }
                firstCol = false;
                sbColumn.AppendLine(string.Format("[{0}]", c.Name));
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
                sb.Append(string.Format("SELECT {0}", sbColumn.ToString()));
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
            StringBuilder sbColumn = new StringBuilder();
            StringBuilder sbParam = new StringBuilder();
            StringBuilder sbWhere = new StringBuilder();
            bool hasPrimary = false;
            bool firstCol = true;
            bool firstPrimary = true;
            foreach (smo.Column c in t.Columns)
            {
                if (!firstCol)
                {
                    sbColumn.Append("\t, ");
                }
                firstCol = false;
                sbColumn.AppendLine(string.Format("[{0}]", c.Name));
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
                sb.Append(string.Format("SELECT {0}", sbColumn.ToString()));
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

    }
}
