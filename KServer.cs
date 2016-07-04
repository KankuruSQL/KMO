using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using smo = Microsoft.SqlServer.Management.Smo;

namespace KMO
{
    public static class KServer
    {
        #region SQL Server Versions
        /// <summary>
        /// Get Sql server version name from major and minor version
        /// </summary>
        /// <param name="s">your smo server object</param>
        /// <returns>a string</returns>
        public static string VersionName(this smo.Server s)
        {
            string version;
            switch (s.VersionMajor.ToString() + "." + s.VersionMinor.ToString())
            {
                case "13.0":
                    version = "Sql Server 2016";
                    break;
                case "12.0":
                    version = "Sql Server 2014";
                    break;
                case "11.0":
                    version = "Sql Server 2012";
                    break;
                case "10.50":
                    version = "Sql Server 2008 R2";
                    break;
                case "10.0":
                    version = "Sql Server 2008";
                    break;
                case "9.0":
                    version = "Sql Server 2005";
                    break;
                case "8.0":
                    version = "Sql Server 2000";
                    break;
                case "7.0":
                    version = "Sql Server 7";
                    break;
                default:
                    version = "Unknown edition";
                    break;
            }
            return version;
        }

        /// <summary>
        /// Get Sql server full version name from major and minor version, productLevel, Edition and version
        /// </summary>
        /// <param name="s">your smo server object</param>
        /// <returns>a string</returns>
        public static string VersionFull(this smo.Server s)
        {
            return string.Format("{0} {1} {2} ({3})", s.VersionName(), s.ProductLevel, s.Edition, s.Version);
        }

        #endregion

        #region Backups
        /// <summary>
        /// For each DB, get the recovery model, the last backup full and the last restore
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>a dataset with 1 datatable containing the result of the query</returns>
        public static DataSet GetBackupHistory(this smo.Server s)
        {
            smo.Database d = s.Databases["msdb"];
            return d.ExecuteWithResults(@"SELECT sdb.name AS DatabaseName
    , MAX(sdb.recovery_model_desc) as recoveryModel
    , MAX(bus.backup_finish_date) AS LastBackUpTime
	, MAX(rh.restore_date) as LastRestoreTime
FROM sys.databases sdb (NOLOCK)
    LEFT OUTER JOIN msdb.dbo.backupset bus (NOLOCK) ON bus.database_name = sdb.name 
		AND COALESCE(bus.is_snapshot, 0) != 1 
		AND COALESCE(bus.type, 'D') = 'D' 
    LEFT join msdb.dbo.restorehistory rh (NOLOCK) on rh.destination_database_name = sdb.Name
GROUP BY sdb.name
ORDER BY sdb.name");
        }
        #endregion

        #region Logins and Security
        /// <summary>
        /// Get all login with sysadmin rights
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>the result of the query</returns>
        public static DataSet GetWhoIsSa(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"SELECT mem.name
	, mem.type_desc
	, mem.create_date
	, mem.modify_date
	, mem.is_disabled
FROM sys.server_role_members AS srm (NOLOCK)
	INNER JOIN sys.server_principals AS mem (NOLOCK) ON mem.principal_id = srm.member_principal_id
	INNER JOIN sys.server_principals AS rol (NOLOCK) ON rol.principal_id = srm.role_principal_id
WHERE rol.name = 'sysadmin'
ORDER BY mem.name");
        }
        #endregion
    }
}
