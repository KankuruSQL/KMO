using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using smo = Microsoft.SqlServer.Management.Smo;

namespace KMO
{
    public static class KDatabase
    {
        public static bool IsDistributor(this smo.Database d)
        {
            string sql = string.Format("SELECT 1 FROM sys.databases WHERE is_distributor = 1 AND name = '{0}'", d.Name);
            DataSet ds = d.ExecuteWithResults(sql);
            return (ds.Tables[0].Rows.Count == 1);
        }
    }
}
