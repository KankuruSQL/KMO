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

        #region Monitoring
        /// <summary>
        /// Get informations about actives session. Locks, performances issues, percent complete, execution plans, etc...
        /// </summary>
        /// <param name="s">Your smo Server</param>
        /// <param name="WithSystemSession">True if you want to select system session. False if you only need user sessions</param>
        /// <param name="WithQueryPlan">True if you want to fill the last column with query plan (xml format). False without execution plan and better performances</param>
        /// <returns>The result of the query in a dataset</returns>
        public static DataSet GetLiveSession(this smo.Server s, bool WithSystemSession = false, bool WithQueryPlan = false)
        {
            smo.Database d = s.Databases["master"];
            string _sql = @"SELECT CAST(qe.session_id AS VARCHAR) AS [Kill]
    , CASE WHEN CAST(blocking_session_id AS VARCHAR) = '0' THEN '' ELSE CAST(blocking_session_id AS VARCHAR) END AS [Blocking_Session_Info]
	, s.login_name AS [Login]
	, s.host_name AS [Host_Name]
	, s.program_name AS [Program_Name]
	, s.client_interface_name AS [Client_Interface_Name]
	, db_name(qe.Database_id) AS [Database]
	, qe.logical_reads AS [Logical_Read]
	, qe.cpu_time AS [CPU_Time]
	, DATEDIFF(MINUTE, start_time, getdate()) AS [Duration]
	, command AS [Command]
	, qe.status AS [Status]
	, percent_complete AS [Percent_Complete]
	, start_time AS [Start_Time]
    , qe.open_transaction_count AS [Open_Transaction_Count]
    , a.text AS [Query]
    , {0} AS [Execution_Plan]
FROM sys.dm_exec_requests qe (NOLOCK)
	INNER JOIN sys.dm_exec_sessions s (NOLOCK) on qe.session_id = s.session_id 
	LEFT JOIN (select sqe.session_id
					, st.text
				FROM sys.dm_exec_requests sqe (NOLOCK)
					CROSS APPLY sys.dm_exec_sql_text(sqe.sql_handle) st) a ON qe.session_id = a.session_id
{1}
WHERE qe.session_id != @@SPID
	{2}
ORDER BY blocking_session_id DESC
	, Duration DESC
	, [CPU_Time] DESC";
            string _sql0 = "''";
            string _sql1 = string.Empty;
            string _sql2 = string.Empty;
            if (WithQueryPlan)
            {
                _sql0 = "b.query_plan";
                _sql1 = @"	LEFT JOIN (select sqe.session_id
					, ph.query_plan
				FROM sys.dm_exec_requests sqe (NOLOCK)
					CROSS APPLY sys.dm_exec_query_plan(sqe.plan_handle) ph) b ON qe.session_id = b.session_id";
            }
            if (!WithSystemSession)
            {
                _sql2 = "AND s.is_user_process = 1";
            }
            _sql = string.Format(_sql, _sql0, _sql1, _sql2);
            return d.ExecuteWithResults(_sql);
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
        /// <summary>
        /// Get information (size, freespace, name, last autogrowth) for each databases files.
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="minSize">Use this param to hide files smaller than this size in Mb(0 by default)</param>
        /// <param name="maxSize">Use this param to hide files greater than this size in Mb(2147483647 by default)</param>
        /// <param name="withData">To get data files (true by default)</param>
        /// <param name="withLog">To get log files (true by default)</param>
        /// <returns>a dataset</returns>
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

        #region CPU
        /// <summary>
        /// Get the CPU usage by database from query execution statistics
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>return the result of the query in a dataset</returns>
        public static DataSet GetCPUbyDatabase(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"WITH DB_CPU_Stats AS
(
	SELECT DatabaseID
		, DB_Name(DatabaseID) AS DatabaseName
		, SUM(total_worker_time) / 1000 AS CPU_Time_Ms
    FROM sys.dm_exec_query_stats AS qs (NOLOCK)
    CROSS APPLY (SELECT CONVERT(int, value) AS DatabaseID
                FROM sys.dm_exec_plan_attributes(qs.plan_handle)
                WHERE attribute = N'dbid') AS F_DB
    GROUP BY DatabaseID
)
SELECT DatabaseName
	, CPU_Time_Ms
	, CAST(CPU_Time_Ms * 1.0 / SUM(CPU_Time_Ms) OVER() * 100.0 AS DECIMAL(5, 2)) AS CPUPercent
	
FROM DB_CPU_Stats 
WHERE DatabaseID != 32767 
ORDER BY ROW_NUMBER() OVER(ORDER BY CPU_Time_Ms DESC) OPTION (RECOMPILE)";
            return d.ExecuteWithResults(sql);
        }
        
        /// <summary>
        /// Get the CPU usage history (256 last ticks) from dm_os_ring_buffers
        /// This dmv is not supported by Microsoft...
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DataSet GetCPUFromRing(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"DECLARE @ts_now bigint
SELECT @ts_now = cpu_ticks / (cpu_ticks / ms_ticks)
FROM sys.dm_os_sys_info
;WITH ring AS
(
    SELECT
       record.value('(Record/@id)[1]', 'int') AS record_id,
       DATEADD (ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS EventTime,
       100-record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_post_sp2,
       record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_post_sp2 ,
       100-record.value('(Record/SchedluerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_pre_sp2,
       record.value('(Record/SchedluerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_pre_sp2
     FROM (
       SELECT timestamp, CONVERT (xml, record) AS record
       FROM sys.dm_os_ring_buffers
       WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
         AND record LIKE '%<SystemHealth>%') AS t
), cte AS
(
    SELECT EventTime
        , CASE WHEN system_cpu_utilization_post_sp2 IS NOT NULL THEN system_cpu_utilization_post_sp2 ELSE system_cpu_utilization_pre_sp2 END AS system_cpu
        , CASE WHEN sql_cpu_utilization_post_sp2 IS NOT NULL THEN sql_cpu_utilization_post_sp2 ELSE sql_cpu_utilization_pre_sp2 END AS sql_cpu
    FROM ring
)
SELECT EventTime
    , system_cpu
    , CASE WHEN sql_cpu > system_cpu THEN sql_cpu / 2 ELSE sql_cpu END AS sql_cpu
FROM cte
ORDER BY EventTime DESC";
            return d.ExecuteWithResults(sql);
        }
        #endregion
    }
}
