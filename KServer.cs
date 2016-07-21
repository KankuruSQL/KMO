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

        #region Disk Space
        public static DataSet GetFileTreeMaps(this smo.Server s, int minSize = 0, int maxSize = 2147483647, bool withData = true, bool withLog = true)
        {
            smo.Database d = s.Databases["master"];
            string fileTypeFilter = string.Empty;

            if (!withData)
            {
                fileTypeFilter += " AND b.TYPE != 0 ";
            }
            if (!withLog)
            {
                fileTypeFilter += " AND b.TYPE = 0 ";
            }

            string sql = string.Format(@"CREATE TABLE #TMPLASTFILECHANGE 
(
	databasename nvarchar(128)
	, filename nvarchar(128)
	, endtime datetime
)

DECLARE @path NVARCHAR(1000)
SELECT @path = SUBSTRING(PATH, 1, LEN(PATH) - CHARINDEX('\', REVERSE(PATH))) + '\log.trc'
FROM sys.traces
WHERE id = 1;

WITH CTE (databaseid, filename, EndTime)
AS
(
	SELECT databaseid
		, filename
		, MAX(t.EndTime) AS EndTime
	FROM ::fn_trace_gettable(@path, default ) t
	WHERE EventClass IN (92, 93)
		AND DATEDIFF(hh,StartTime,GETDATE()) < 24
	GROUP BY databaseid
		, filename
)
INSERT INTO #TMPLASTFILECHANGE
(
	databasename
	, filename
	, endtime
)
SELECT DB_NAME(database_id) AS DatabaseName
	, mf.name AS LogicalName
	, cte.EndTime
FROM sys.master_files mf
	LEFT JOIN CTE cte on mf.database_id=cte.databaseid 
		AND mf.name=cte.filename
WHERE cte.EndTime IS NOT NULL

CREATE TABLE #TMPSPACEUSED 
(
	DBNAME    NVARCHAR(128),
	FILENAME   NVARCHAR(128),
	SPACEUSED FLOAT
)

INSERT INTO #TMPSPACEUSED
(
	DBNAME
	, FILENAME
	, SPACEUSED
)
EXEC('sp_MSforeachdb''use [?]; Select ''''?'''' AS DBName
			, Name AS FileNme
			, fileproperty(Name,''''SpaceUsed'''') AS SpaceUsed 
		FROM sys.sysfiles''')

SELECT a.name AS DatabaseName
	, b.name AS FileName
	, CASE b.TYPE WHEN 0 THEN 'DATA' ELSE b.type_desc END AS FileType
	, CAST((b.size * 8 / 1024.0) AS DECIMAL(18,2)) AS FileSize
	, CAST((b.size * 8 / 1024.0) - (d.SPACEUSED / 128.0) AS DECIMAL(15,2)) / CAST((b.size * 8 / 1024.0) AS DECIMAL(18,2)) * 100 AS FreeSpace
	, b.physical_name
	, datediff(DAY, c.endtime, GETDATE()) AS LastGrowth
FROM sys.databases a
	INNER JOIN sys.master_files b ON a.database_id = b.database_id
	INNER JOIN #TMPSPACEUSED d  ON a.NAME = d.DBNAME 
		AND b.name = d.FILENAME
	LEFT JOIN #TMPLASTFILECHANGE c on a.name = c.databasename 
		AND b.name = c.filename
WHERE b.size >= ({0} / 8.0 * 1024.0)
	AND b.size <= ({1} / 8.0 * 1024.0)
    {2}
ORDER BY FILESIZE DESC

DROP TABLE #TMPSPACEUSED
DROP TABLE #TMPLASTFILECHANGE
", minSize, maxSize, fileTypeFilter);
            return d.ExecuteWithResults(sql);
        }
        #endregion
    }
}
