﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using static KMO.Enums;
using smo = Microsoft.SqlServer.Management.Smo;

namespace KMO
{
    public static class KServer
    {

        #region General informations
        /// <summary>
        /// Get the machine type : this server is a physical server or a Virtual Machine ?
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>Physical, virtual or unknown</returns>
        public static string MachineType(this smo.Server s)
        {
            string serverType = "Unknown";
            smo.Database d = s.Databases["master"];
            string sql = @"IF EXISTS(SELECT *
FROM sys.all_columns
WHERE name = 'virtual_machine_type_desc'
AND OBJECT_NAME(object_id) = 'dm_os_sys_info')
BEGIN
EXECUTE sys.sp_executesql N'SELECT virtual_machine_type_desc FROM sys.dm_os_sys_info'
END
";
            DataTable dt = d.ExecuteWithResults(sql).Tables[0];
            if (dt != null && dt.Rows.Count > 0)
            {
                string virtualType = dt.Rows[0]["virtual_machine_type_desc"].ToString();
                switch (virtualType)
                {
                    case "NONE":
                        serverType = "Physical server";
                        break;
                    case "HYPERVISOR":
                        serverType = "Virtual server";
                        break;
                }
            }
            return serverType;
        }

        /// <summary>
        /// When Sql server service restarts, TempDB is recreated. So tempdb create date can be used to know when the server restarted.
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>the datetime when sql service restart</returns>
        public static DateTime LastRestart(this smo.Server s)
        {
            return s.Databases["tempdb"].CreateDate;
        }

        /// <summary>
        /// Get services account with status
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>a datatable with service name + service informations</returns>
        public static DataTable ServiceStatus(this smo.Server s)
        {
            string sql = @"SELECT servicename
    , service_account + ' (' + status_desc + ')' AS serviceInfo
FROM sys.dm_server_services";
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get databases with compatibility level lower than instance
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>a datatable with the list of databases</returns>
        public static DataTable DatabaseLowCompatibilityLevel(this smo.Server s)
        {
            int normalCompatibilityLevel = s.VersionMajor*10;
            string sql = string.Format(@"SELECT name
    , compatibility_level
FROM sys.databases
WHERE compatibility_level != {0}", normalCompatibilityLevel);
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get list of active trace flags
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>a datatable with the list of databases</returns>
        public static DataTable GetActiveTraceFlags(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"CREATE TABLE #traceflag (traceflag int, status int, global int, session int)
INSERT INTO #traceflag
EXEC ('DBCC TRACESTATUS(-1)')
SELECT * FROM #traceflag
DROP TABLE #traceflag").Tables[0];
        }

        #endregion

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
                case "15.0":
                    version = "Sql Server 2019";
                    break;
                case "14.0":
                    version = "Sql Server 2017";
                    break;
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
        /// <returns>The result of the query in a DataTable</returns>
        public static DataTable GetLiveSession(this smo.Server s, bool WithSystemSession = false, bool WithQueryPlan = false)
        {
            smo.Database d = s.Databases["master"];
            string _sql = @"SELECT CAST(qe.session_id AS VARCHAR) AS [Session Id]
    , CASE WHEN blocking_session_id = 0 THEN '' ELSE CAST(blocking_session_id AS VARCHAR) END AS [Blocking Session Id]
	, s.login_name AS [Login]
	, s.host_name AS [Host Name]
	, s.program_name AS [Program Name]
	, s.client_interface_name AS [Client Interface Name]
	, db_name(qe.Database_id) AS [Database]
	, qe.logical_reads AS [Logical Read]
    , qe.writes AS [Writes]
	, qe.cpu_time AS [CPU Time]
	, DATEDIFF(MINUTE, start_time, getdate()) AS [Duration]
	, command AS [Command]
	, qe.status AS [Status]
	, percent_complete AS [Percent Complete]
	, start_time AS [Start Time]
    , qe.open_transaction_count AS [Open Transaction Count]
    , qe.row_count AS [Row Count]
    , CASE qe.transaction_isolation_level
        WHEN 1 THEN 'ReadUncomitted'
        WHEN 2 THEN 'ReadCommitted'
        WHEN 3 THEN 'Repeatable'
        WHEN 4 THEN 'Serializable'
        WHEN 5 THEN 'Snapshot'
        ELSE 'Unknown' END AS [Transaction Isolation Level]
    , a.text AS [Query]
    {0}
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
	, qe.cpu_time DESC";
            string _sql0 = "";
            string _sql1 = string.Empty;
            string _sql2 = string.Empty;
            if (WithQueryPlan)
            {
                _sql0 = " , b.query_plan AS [__execPlan]";
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
            return d.ExecuteWithResults(_sql).Tables[0];
        }

        /// <summary>
        /// Get informations about all sessions on a server
        /// </summary>
        /// <param name="s">Your smo Server</param>
        /// <returns>The result of the query in a DataTable</returns>
        public static DataTable GetWho(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"SELECT sp.spid
	, sp.blocked AS Blocked
	, d.name as [Database]
	, RTRIM(sp.hostname) AS [Hostname]
	, RTRIM(sp.program_name) AS [Program Name]
	, RTRIM(sp.loginame) AS [Login Name]
	, RTRIM(sp.nt_domain) AS [NT Domain]
	, RTRIM(sp.nt_username) AS [NT Username]
    , ec.auth_scheme AS [Auth protocol]
	, sp.waittime AS [Wait Time]
	, RTRIM(sp.lastwaittype) AS [Last Wait Type]
	, sp.cpu as [CPU]
	, sp.physical_io AS [Physical IO]
	, sp.memusage AS [Memory]
	, sp.login_time AS [Login Time]
	, sp.last_batch AS [Last Batch]
	, sp.ecid AS [Execution Context ID]
	, sp.open_tran AS [Open Tran]
	, RTRIM(sp.status) AS [Status]
	, sp.hostprocess AS [Host Process]
	, RTRIM(sp.cmd) AS [Command Type]
	, sp.net_address AS [NET Adress]
	, RTRIM(sp.net_library) AS [NET Library]
	, sp2.spid AS [Blocking]
FROM master.dbo.sysprocesses sp
	LEFT JOIN master.dbo.sysdatabases d ON sp.dbid = d.dbid
	LEFT JOIN master.dbo.sysusers u ON sp.uid = u.uid
	LEFT JOIN master.dbo.sysprocesses sp2 ON sp.spid = sp2.blocked
    LEFT JOIN sys.dm_exec_connections ec ON sp.spid = ec.session_id AND ec.parent_connection_id IS NULL
ORDER BY sp.blocked
    , d.name
    , sp.cpu DESC").Tables[0];
        }

        /// <summary>
        /// Get Stored procedures execution statistics.
        /// Useful to calculate diff statistics like in Live SP Profiler in Kankuru
        /// </summary>
        /// <param name="s">SMO server</param>
        /// <returns>a DataTable</returns>
        public static DataTable GetLiveSPProfiler(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string _sql = @"SELECT database_id AS [Database ID]
	, DB_NAME(database_id) AS [Database Name]
	, object_id AS [Object ID]
	, OBJECT_NAME(object_id, database_id) AS [Object Name]
	--, plan_handle AS [Plan Handle]
	, SUM(total_worker_time) AS [Cpu]
	, SUM(total_elapsed_time) AS [Elapsed Time]
	, SUM(execution_count) AS [Execution Count]
	, SUM(total_logical_reads) AS [Logical Reads]
	, SUM(total_physical_reads) AS [Physical Reads]
	, SUM(total_logical_writes) AS [Logical Writes]
FROM sys.dm_exec_procedure_stats
WHERE database_id != 32767
GROUP BY database_id
	, object_id
	--, plan_handle";
            return d.ExecuteWithResults(_sql).Tables[0];
        }

        /// <summary>
        /// Get queries execution statistics.
        /// Useful to calculate diff statistics like in Live Queries Profiler in Kankuru
        /// </summary>
        /// <param name="s">SMO server</param>
        /// <returns>a DataTable</returns>
        public static DataTable GetLiveQueriesProfiler(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT COALESCE(qt.dbid, 0) AS [Database ID]
	, COALESCE(DB_NAME(qt.dbid), 'NC.') AS [Database Name]
	, qt.text AS [Query]
    , SUM(total_worker_time) AS [Cpu]
    , SUM(total_elapsed_time) AS [Elapsed Time]
    , SUM(execution_count) AS [Execution Count]
    , MIN(qs.execution_count) AS [Unique Execution Count]
    , SUM(total_logical_reads) AS [Logical Reads]
    , SUM(total_physical_reads) AS [Physical Reads]
    , SUM(total_logical_writes) AS [Logical Writes]
FROM sys.dm_exec_query_stats qs
	CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
WHERE qt.dbid != 32767
	OR qt.dbid IS NULL
GROUP BY qt.dbid
	, qt.text";
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get IO statistics.
        /// Useful to calculate diff statistics like in Live IO Profiler in Kankuru
        /// </summary>
        /// <param name="s">SMO server</param>
        /// <returns>a DataTable</returns>
        public static DataTable GetLiveIOProfiler(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string _sql = @"SELECT DB_NAME(fs.database_id) AS [DbName]
	, mf.physical_name AS [FileName]
	, io_stall_read_ms AS [IOStallReadMs]
	, num_of_reads AS [ReadCount]
	, io_stall_write_ms AS [IOStallWriteMs]
	, num_of_writes AS [WriteCount]
	, io_stall_read_ms + io_stall_write_ms AS [IOStallMs]
	, num_of_reads + num_of_writes AS [IOCount]
FROM sys.dm_io_virtual_file_stats(NULL,NULL) AS fs
	INNER JOIN sys.master_files AS mf (NOLOCK) ON fs.database_id = mf.database_id
		AND fs.[file_id] = mf.[file_id]
ORDER BY [IOCount] DESC
OPTION (RECOMPILE)";
            return d.ExecuteWithResults(_sql).Tables[0];
        }
        #endregion

        #region Backups
        /// <summary>
        /// For each DB, get the recovery model, the last full backup date
        /// , the last restore date and the last snapshot date
        /// </summary>
        public static DataTable GetBackupHistory(this smo.Server s)
        {
            smo.Database d = s.Databases["msdb"];
            return d.ExecuteWithResults(@"WITH cteBackup AS(
	SELECT database_name
		, MAX(bus.backup_finish_date) AS LastBackUpTime
	FROM msdb.dbo.backupset bus
	WHERE COALESCE(bus.is_snapshot, 0) != 1
		AND COALESCE(bus.type, 'D') = 'D' 
	GROUP BY database_name
),
cteRestore AS (
	SELECT rh.destination_database_name
		, MAX(rh.restore_date) as LastRestoreTime
	FROM msdb.dbo.restorehistory rh (NOLOCK)
	GROUP BY rh.destination_database_name
),
cteSnapshot AS(
	SELECT database_name
		, MAX(bus.backup_finish_date) AS LastSnapshotTime
	FROM msdb.dbo.backupset bus
	WHERE bus.is_snapshot = 1
	GROUP BY database_name
)
SELECT sdb.name AS DatabaseName
    ,sdb.recovery_model_desc as recoveryModel
	, cb.LastBackUpTime
	, cr.LastRestoreTime
	, cs.LastSnapshotTime
FROM sys.databases sdb (NOLOCK)
	LEFT JOIN cteBackup cb ON sdb.name = cb.database_name
	LEFT JOIN cteRestore cr ON sdb.name = cr.destination_database_name
	LEFT JOIN cteSnapshot cs ON sdb.name = cs.database_name
ORDER BY DatabaseName").Tables[0];
        }
        #endregion

        #region Logins and Security
        /// <summary>
        /// Get all login with sysadmin rights
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>the result of the query</returns>
        public static DataTable GetWhoIsSa(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"SELECT mem.name AS [Login Name]
	, mem.type_desc AS [Login Type]
	, mem.create_date AS [Create Date]
	, mem.modify_date AS [Modify Date]
	, mem.is_disabled AS [Is Disabled]
FROM sys.server_role_members AS srm (NOLOCK)
	INNER JOIN sys.server_principals AS mem (NOLOCK) ON mem.principal_id = srm.member_principal_id
	INNER JOIN sys.server_principals AS rol (NOLOCK) ON rol.principal_id = srm.role_principal_id
WHERE rol.name = 'sysadmin'
ORDER BY mem.name").Tables[0];
        }

        /// <summary>
        /// Get session statistics by login
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>the result of the query</returns>
        public static DataTable GetLoginStats(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"SELECT login_name AS [Login Name]
    , COUNT(session_id) AS [session Count]
    , SUM(cpu_time) AS [Cpu Time]
    , SUM(memory_usage) AS [Memory Usage]
    , SUM(logical_reads) AS [Logical Reads]
    , SUM(writes) AS [Writes]
FROM sys.dm_exec_sessions (NOLOCK)
GROUP BY login_name
ORDER BY COUNT(session_id) DESC").Tables[0];
        }

        /// <summary>
        /// Get connections by client
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>the result of the query</returns>
        public static DataTable GetLoginConnections(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"SELECT es.host_name AS [Host Name]
	, ec.client_net_address AS [Client Net Address]
	, es.program_name AS [Program Name]
	, es.login_name AS [Login Name]
	, ec.auth_scheme AS [Auth Schema]
	, ec.net_transport AS [Net Transport]
	, COUNT(*) AS [Session Count]
FROM sys.dm_exec_sessions es (NOLOCK)
	INNER JOIN sys.dm_exec_connections ec (NOLOCK) ON es.session_id = ec.session_id
GROUP BY es.[host_name]
	, ec.client_net_address
	, es.[program_name]
	, es.login_name
	, ec.auth_scheme
	, ec.net_transport
ORDER BY ec.client_net_address
	, es.[program_name]").Tables[0];
        }
        #endregion

        #region Disk
        /// <summary>
        /// Get information (size, freespace, name, last autogrowth) for each databases files.
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="minSize">Use this param to hide files smaller than this size in Mb(0 by default)</param>
        /// <param name="maxSize">Use this param to hide files greater than this size in Mb(2147483647 by default)</param>
        /// <param name="withData">To get data files (true by default)</param>
        /// <param name="withLog">To get log files (true by default)</param>
        /// <returns>a DataTable</returns>
        public static DataTable GetFileTreeMaps(this smo.Server s, int minSize = 0, int maxSize = 2147483647, bool withData = true, bool withLog = true)
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
SELECT @path = SUBSTRING(path, 1, LEN(path) - CHARINDEX('\', REVERSE(path))) + '\log.trc'
FROM sys.traces
WHERE is_default = 1;

IF(@path IS NOT NULL)
BEGIN
    WITH CTE (databaseid, filename, EndTime)
    AS
    (
	    SELECT DatabaseID
		    , FileName
		    , MAX(t.EndTime) AS EndTime
	    FROM ::fn_trace_gettable(@path, default ) t
	    WHERE EventClass IN (92, 93)
		    AND DATEDIFF(hh,StartTime,GETDATE()) < 24
	    GROUP BY DatabaseID
		    , FileName
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
END

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
	, CASE b.type WHEN 0 THEN 'DATA' ELSE b.type_desc END AS FileType
	, CAST((CONVERT(BigInt,b.size) * 8 / 1024.0) AS DECIMAL(18,2)) AS FileSize
	, CAST((CONVERT(BigInt,b.size) * 8 / 1024.0) - (d.SPACEUSED / 128.0) AS DECIMAL(15,2)) / CAST((CONVERT(BigInt,b.size) * 8 / 1024.0) AS DECIMAL(18,2)) * 100 AS FreeSpace
	, b.physical_name
	, datediff(DAY, c.endtime, GETDATE()) AS LastGrowth
FROM sys.databases a
	INNER JOIN sys.master_files b ON a.database_id = b.database_id
	INNER JOIN #TMPSPACEUSED d  ON a.name = d.DBNAME 
		AND b.name = d.FILENAME
	LEFT JOIN #TMPLASTFILECHANGE c on a.name = c.databasename 
		AND b.name = c.filename
WHERE b.size >= ({0} / 8.0 * 1024.0)
	AND b.size <= ({1} / 8.0 * 1024.0)
    {2}
ORDER BY FileSize DESC

DROP TABLE #TMPSPACEUSED
DROP TABLE #TMPLASTFILECHANGE
", minSize, maxSize, fileTypeFilter);
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get IO statistics by database files.
        /// Useful to detect disk performance issue or high database IO
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetIOStatistics(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(@"SELECT DB_NAME(fs.database_id) AS [Database Name]
	, mf.physical_name AS [Physical Name]
	, io_stall_read_ms AS [IO Stall Read ms]
	, num_of_reads AS [Num of Reads]
	, CASE WHEN num_of_reads = 0 THEN 0
		ELSE CAST(io_stall_read_ms / num_of_reads AS NUMERIC(10,1)) END AS [Avg Read Stall ms]
	, io_stall_write_ms AS [IO Stall Write ms]
	, num_of_writes AS [Num of Writes]
	, CASE WHEN num_of_writes = 0 THEN 0
		ELSE CAST(io_stall_write_ms / num_of_writes AS NUMERIC(10,1)) END AS [Avg Write Stall ms]
	, io_stall_read_ms + io_stall_write_ms AS [IO Stalls]
	, num_of_reads + num_of_writes AS [Total IO]
	, CASE WHEN (num_of_reads + num_of_writes) = 0 THEN 0
		ELSE CAST((io_stall_read_ms + io_stall_write_ms)/(num_of_reads + num_of_writes) AS NUMERIC(10,1)) END AS [Avg IO Stall ms]
FROM sys.dm_io_virtual_file_stats(null,null) AS fs 
	INNER JOIN sys.master_files AS mf (NOLOCK) ON fs.database_id = mf.database_id 
		AND fs.[file_id] = mf.[file_id]
ORDER BY [Avg IO Stall ms] DESC 
OPTION (RECOMPILE)").Tables[0];
        }

        /// <summary>
        /// Get free disk space by logical drive. If Ole automation procedures are enable, free disk space is available in percentage.
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>datable with logical name + free disk space</returns>
        public static DataTable GetFreeDiskSpace(this smo.Server s)
        {
            string sql = @"DECLARE @t TABLE(
	drive VARCHAR(2)
	, TotalSize BIGINT DEFAULT 0
	, FreeSpace BIGINT)
INSERT INTO @t(drive, FreeSpace)
EXEC xp_fixeddrives
SELECT drive
	, TotalSize
	, FreeSpace
FROM @t";
            if (s.Configuration.OleAutomationProceduresEnabled.RunValue == 1)
            {
                sql = @"SET NOCOUNT ON
DECLARE @hr INT
DECLARE @fso INT
DECLARE @drive CHAR(1)
DECLARE @odrive INT
DECLARE @TotalSize VARCHAR(20)
DECLARE @MB NUMERIC(18, 2)
SET @MB = 1048576

CREATE TABLE #drives(
	drive CHAR(1) PRIMARY KEY
	, FreeSpace INT NULL
	, TotalSize INT NULL)

INSERT #drives(drive,FreeSpace)
EXEC master.dbo.xp_fixeddrives

EXEC @hr = sp_OACreate 'Scripting.FileSystemObject', @fso OUT
IF @hr <> 0 EXEC sp_OAGetErrorInfo @fso

DECLARE dcur CURSOR LOCAL FAST_FORWARD
FOR SELECT drive
	FROM #drives
	ORDER by drive
OPEN dcur FETCH NEXT FROM dcur INTO @drive
WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC @hr = sp_OAMethod @fso, 'GetDrive', @odrive OUT, @drive
	IF @hr <> 0 EXEC sp_OAGetErrorInfo @fso
	EXEC @hr = sp_OAGetProperty @odrive, 'TotalSize', @TotalSize OUT
	IF @hr <> 0 EXEC sp_OAGetErrorInfo @odrive

	UPDATE #drives
		SET TotalSize = @TotalSize / @MB
	WHERE drive = @drive
FETCH NEXT FROM dcur INTO @drive
END
CLOSE dcur
DEALLOCATE dcur
EXEC @hr = sp_OADestroy @fso
IF @hr <> 0 EXEC sp_OAGetErrorInfo @fso
SELECT drive
	, TotalSize
	, FreeSpace
FROM #drives
ORDER BY drive
DROP TABLE #drives";
            }
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get the size of each data or log files for each databases
        /// </summary>
        /// <param name="s">the smo server object</param>
        /// <param name="fileType">Enum available in KMO.Enums</param>
        public static DataTable GetFileSize(this smo.Server s, FileType fileType)
        {
            string sqlFileType = " WHERE b.type != 1 ";
            if(fileType == FileType.Logs)
            {
                sqlFileType = " WHERE b.type = 1 ";
            }
            string sql = string.Format(@"CREATE TABLE #TMPLASTFILECHANGE 
(
	databasename nvarchar(128)
	, filename nvarchar(128)
	, endtime datetime
)

DECLARE @path NVARCHAR(1000)
SELECT @path = SUBSTRING(path, 1, LEN(path) - CHARINDEX('\', REVERSE(path))) + '\log.trc'
FROM sys.traces
WHERE is_default = 1;

IF(@path IS NOT NULL)
BEGIN
    WITH CTE (databaseid, filename, EndTime)
    AS
    (
	    SELECT DatabaseID
		    , FileName
		    , MAX(t.EndTime) AS EndTime
	    FROM ::fn_trace_gettable(@path, default ) t
	    WHERE EventClass IN (92, 93)
		    AND DATEDIFF(hh,StartTime,GETDATE()) < 24
	    GROUP BY DatabaseID
		    , FileName
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
END

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

SELECT a.name AS [Database]
	, b.name AS [Logical File Name]
	, a.recovery_model_desc AS [Recovery Model]
	, CASE WHEN a.database_id = 2 THEN CAST((z.size * 8 / 1024.0) AS DECIMAL(18,2)) ELSE CAST((b.size * 8 / 1024.0) AS DECIMAL(18,2)) END AS [File Size]
	, CAST((d.SPACEUSED / 128.0) AS DECIMAL(15,2)) AS [Used Space MB]
	, CASE WHEN a.database_id = 2 THEN CAST((z.size * 8 / 1024.0) - (d.SPACEUSED / 128.0) AS DECIMAL(15,2)) ELSE CAST((b.size * 8 / 1024.0) - (d.SPACEUSED / 128.0) AS DECIMAL(15,2)) END AS [Free Space MB]
	, CASE WHEN a.database_id = 2 THEN CAST((z.size * 8 / 1024.0) - (d.SPACEUSED / 128.0) AS DECIMAL(15,2)) / CAST((z.size * 8 / 1024.0) AS DECIMAL(18,2)) * 100 ELSE CAST((b.size * 8 / 1024.0) - (d.SPACEUSED / 128.0) AS DECIMAL(15,2)) / CAST((b.size * 8 / 1024.0) AS DECIMAL(18,2)) * 100 END AS [Free Space %]
	, c.endtime AS [Last Auto Growth]
	, b.physical_name AS [Physical Name]
	, CASE WHEN a.state != 0 THEN '#33FF5B66' END AS __rowColor
FROM sys.databases a
	INNER JOIN sys.master_files b ON a.database_id = b.database_id
    LEFT JOIN tempdb.sys.database_files z with (nolock) ON b.file_id = z.file_id
        AND b.database_id = 2
	LEFT JOIN #TMPSPACEUSED d  ON a.name = d.DBNAME 
		AND b.name = d.FILENAME
	LEFT JOIN #TMPLASTFILECHANGE c on a.name = c.databasename 
		AND b.name = c.filename
{0}
ORDER BY [File Size] DESC

DROP TABLE #TMPSPACEUSED
DROP TABLE #TMPLASTFILECHANGE", sqlFileType);
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #endregion

        #region CPU
        /// <summary>
        /// Get the CPU usage by database from query execution statistics
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetCPUbyDatabase(this smo.Server s)
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
SELECT DatabaseName AS [Database Name]
	, CPU_Time_Ms AS [CPU Time Ms]
	, CAST(CPU_Time_Ms * 1.0 / SUM(CPU_Time_Ms) OVER() * 100.0 AS DECIMAL(5, 2)) AS [CPU Percent]	
FROM DB_CPU_Stats 
WHERE DatabaseID != 32767 
ORDER BY ROW_NUMBER() OVER(ORDER BY CPU_Time_Ms DESC) OPTION (RECOMPILE)";
            return d.ExecuteWithResults(sql).Tables[0];
        }
        
        /// <summary>
        /// Get the CPU usage history (256 last ticks) from dm_os_ring_buffers
        /// This dmv is not supported by Microsoft...
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DataTable GetCPUFromRing(this smo.Server s)
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
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Returns the list of windows process. Not yet tested on linux
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DataTable GetProcessCPU(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"EXEC xp_cmdshell 'powershell ""get-process | Select-Object ProcessName, CPU""'";
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion

        #region Memory
        /// <summary>
        /// Get Buffer Size by Database
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetBufferByDatabase(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT CASE WHEN database_id = 32767 THEN 'Resource' ELSE DB_NAME(database_id) END AS [Database]
	, COUNT(*) / 128 AS [Cached_Size]
FROM sys.dm_os_buffer_descriptors (NOLOCK)
GROUP BY database_id
ORDER BY [Cached_Size] DESC
OPTION (RECOMPILE)";
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion

        #region Waits
        /// <summary>
        /// Get Wait statistics for an instance. Very helpful to understand performance issues
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="listWaitToIgnore">List of wait stats to ignore with simple quote and separated by coma. Example : 'WaitStatToIgnore1', 'WaitStatToIgnore2' </param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetWaitStatistics(this smo.Server s, string listWaitToIgnore = "'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP', 'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE', 'CHKPT', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'CLR_SEMAPHORE', 'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE', 'DBMIRRORING_CMD', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE', 'EXECSYNC', 'FSAGENT', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX', 'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE', 'HADR_TIMER_TASK', 'HADR_WORK_QUEUE', 'KSOURCE_WAKEUP', 'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'ONDEMAND_TASK_QUEUE', 'PWAIT_ALL_COMPONENTS_INITIALIZED', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 'REQUEST_FOR_DEADLOCK_SEARCH', 'RESOURCE_QUEUE', 'SERVER_IDLE_CHECK', 'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP', 'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY', 'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK', 'SLEEP_TASK', 'SLEEP_TEMPDBSTARTUP', 'SNI_HTTP_ACCEPT', 'SP_SERVER_DIAGNOSTICS_SLEEP', 'SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES', 'WAIT_FOR_RESULTS', 'WAITFOR', 'WAITFOR_TASKSHUTDOWN', 'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_CKPT_CLOSE', 'XE_DISPATCHER_JOIN', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'")
        {
            smo.Database d = s.Databases["master"];
            string sqlIgnore = string.Empty;
            if (!string.IsNullOrEmpty(listWaitToIgnore))
            {
                sqlIgnore = "AND [wait_type] NOT IN (" + listWaitToIgnore + ")";
            }
            string sql = string.Format(@"WITH [Waits] AS
(
	SELECT
		[wait_type],
		[wait_time_ms] / 1000.0 AS [WaitS],
		([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
		[signal_wait_time_ms] / 1000.0 AS [SignalS],
		[waiting_tasks_count] AS [WaitCount],
		100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
		ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum],
        CASE WHEN CHARINDEX('_', wait_type, 0) != 0 THEN SUBSTRING(wait_type,0, CHARINDEX('_', wait_type, 0)) ELSE wait_type END AS [Category]
	FROM sys.dm_os_wait_stats
	WHERE [waiting_tasks_count] > 0
		{0}
)
SELECT
	MAX ([W1].[wait_type]) AS [Wait Type]
	, MAX ([W1].[Category]) AS [Category]
	, MAX ([W1].[WaitCount]) AS [Tasks Count]
	, CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait Time]
	, CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Wait Percent]
FROM [Waits] AS [W1]
	INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX ([W1].[Percentage]) < 99;", sqlIgnore);
            DataTable dt = d.ExecuteWithResults(sql).Tables[0];
            dt.Columns.Add(new DataColumn("Description"));
            foreach (DataRow dr in dt.Rows)
            {
                string waitType = dr["Wait Type"].ToString();
                dr["Description"] = (from l in Resources.WaitTypes.GetWaitsDictionary()
                                     where l.Key == waitType
                                     select l.Value).SingleOrDefault();
            }
            return dt;
        }

        /// <summary>
        /// Get Wait statistics for an instance. Used in Live Wait Profiler
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="listWaitToIgnore">Optionnal List of wait stats to ignore with simple quote and separated by coma. Example : 'WaitStatToIgnore1', 'WaitStatToIgnore2' </param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetLiveWaitProfiler(this smo.Server s, string listWaitToIgnore = "'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP', 'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE', 'CHKPT', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'CLR_SEMAPHORE', 'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE', 'DBMIRRORING_CMD', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE', 'EXECSYNC', 'FSAGENT', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX', 'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE', 'HADR_TIMER_TASK', 'HADR_WORK_QUEUE', 'KSOURCE_WAKEUP', 'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'ONDEMAND_TASK_QUEUE', 'PWAIT_ALL_COMPONENTS_INITIALIZED', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 'REQUEST_FOR_DEADLOCK_SEARCH', 'RESOURCE_QUEUE', 'SERVER_IDLE_CHECK', 'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP', 'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY', 'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK', 'SLEEP_TASK', 'SLEEP_TEMPDBSTARTUP', 'SNI_HTTP_ACCEPT', 'SP_SERVER_DIAGNOSTICS_SLEEP', 'SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES', 'WAIT_FOR_RESULTS', 'WAITFOR', 'WAITFOR_TASKSHUTDOWN', 'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_CKPT_CLOSE', 'XE_DISPATCHER_JOIN', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'")
        {
            string sqlIgnore = string.Empty;
            if (!string.IsNullOrEmpty(listWaitToIgnore))
            {
                sqlIgnore = "AND [wait_type] NOT IN (" + listWaitToIgnore + ")";
            }
            smo.Database d = s.Databases["master"];
            string sql = string.Format(@"SELECT wait_type
	, CAST(wait_time_ms / 1000.0 AS DECIMAL (16,2)) AS [Wait]
	, CAST((wait_time_ms - signal_wait_time_ms) / 1000.0 AS DECIMAL (16,2)) AS [Resource]
	, CAST(signal_wait_time_ms / 1000.0 AS DECIMAL (16,2)) AS [Signal]
	, waiting_tasks_count AS [Count]
    , CASE WHEN CHARINDEX('_', wait_type, 0) != 0 THEN SUBSTRING(wait_type,0, CHARINDEX('_', wait_type, 0)) ELSE wait_type END AS [Category]
FROM sys.dm_os_wait_stats
WHERE waiting_tasks_count > 0
{0}
ORDER BY wait_type", sqlIgnore);
            DataTable dt = d.ExecuteWithResults(sql).Tables[0];
            dt.Columns.Add(new DataColumn("Description"));
            foreach (DataRow dr in dt.Rows)
            {
                string waitType = dr["wait_type"].ToString();
                dr["Description"] = (from l in Resources.WaitTypes.GetWaitsDictionary()
                                       where l.Key == waitType
                                       select l.Value).SingleOrDefault();
            }
            return dt;
        }

        /// <summary>
        /// Get lock statistics for an instance. Used in Live Lock Profiler
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetLiveLockProfiler(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @";WITH cte AS
(
    SELECT CAST(qe.session_id AS VARCHAR) AS [Session_Id]
        , blocking_session_id
        , db_name(qe.Database_id) AS [DatabaseName]
        , s.login_name
        , s.host_name
        , s.program_name
        , s.client_interface_name
        , start_time
		, qe.logical_reads
		, qe.writes
		, qe.cpu_time
        , qe.command
        , a.text AS [Query]
    FROM sys.dm_exec_requests qe (NOLOCK)
        INNER JOIN sys.dm_exec_sessions s (NOLOCK) on qe.session_id = s.session_id 
        LEFT JOIN (select sqe.session_id
                        , st.text
                    FROM sys.dm_exec_requests sqe (NOLOCK)
                        CROSS APPLY sys.dm_exec_sql_text(sqe.sql_handle) st) a ON qe.session_id = a.session_id
    WHERE qe.session_id != @@SPID
)
SELECT c.Session_Id
    , c.DatabaseName
    , c.login_name
    , c.host_name
    , c.program_name
    , c.client_interface_name
    , c.command
    , c.Start_Time
	, c.cpu_time
	, c.logical_reads
	, c.writes
    , c.Query
	, c.blocking_session_id
	, c2.blocking_session_id AS blocking_session_id_Lvl2
	, db_name(s.dbid) AS blocking_DatabaseName
	, s.loginame as blocking_loginname
	, s.hostname as blocking_hostname
	, s.program_name as blocking_program_name
	, c2.client_interface_name as blocking_client_interface_name
	, c2.start_time AS blocking_start_time
	, coalesce(c2.query, text2.text) AS blocking_Query
    , s.cpu AS blocking_cpu
	, s.cmd AS blocking_command
FROM cte c
    LEFT JOIN cte c2 on c.blocking_session_id = c2.Session_Id
    LEFT JOIN master.sys.sysprocesses s on c.blocking_session_id = s.spid
	LEFT JOIN(
		SELECT s2.spid
			, st2.text
		FROM master.sys.sysprocesses s2
			CROSS APPLY sys.dm_exec_sql_text(s2.sql_handle) st2
	) AS text2 ON s.spid = text2.spid
WHERE c.blocking_session_id != 0";
            DataTable dt = d.ExecuteWithResults(sql).Tables[0];
            return dt;
        }

        #endregion

        #region ErrorLogs
        /// <summary>
        /// Read ErrorLog files with some improvements :
        /// Doesn't return successful or failed login
        /// Remove useless message
        /// Filter by date
        /// Filter Informationnal messages
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <param name="startTime">Doesn't return events before this datetime. You could use DateTime.MinValue if you don't want to filter.</param>
        /// <param name="endTime">Doesn't return events after this datetime. You could use DateTime.MaxValue if you don't want to filter.</param>
        /// <param name="logNumber">The file you want to parse. File 0 by default</param>
        /// <param name="withInformationMessage">Include informationnal messages.</param>
        /// <returns>a DataTable with the result of the query</returns>
        public static DataTable ReadErrorLog(this smo.Server s, DateTime startTime, DateTime endTime, int logFileNumber = 0, bool withInformationMessage = false)
        {
            string sql = string.Format(@"CREATE TABLE #KMOErrorLog
(
    LogDate DATETIME
    , ProcessInfo NVARCHAR(50)
    ,vchMessage NVARCHAR(2000)
)
CREATE TABLE #err_log_tmp(ArchiveNo int, CreateDate nvarchar(44), Size int)
INSERT INTO #err_log_tmp EXEC master.dbo.sp_enumerrorlogs
IF(SELECT COUNT(*) AS FileExists
FROM #err_log_tmp
WHERE ArchiveNo = {0}) = 1
BEGIN
    INSERT INTO #KMOErrorLog(LogDate, ProcessInfo, vchMessage)
    EXEC master.dbo.xp_readerrorlog {0}
END
DROP TABLE #err_log_tmp

DELETE FROM #KMOErrorLog 
WHERE LogDate < '{1}'
    OR LogDate > '{2}'
    OR ProcessInfo = 'Logon'
    OR vchMessage LIKE 'Error: %, Severity: %, State: %.'", logFileNumber, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"));

            if (!withInformationMessage)
            {
                sql += @" 
DELETE FROM #KMOErrorLog
WHERE vchMessage LIKE '%This is an informational message%'
    OR vchMessage LIKE '%DBCC CHECKDB%found 0 errors and repaired 0 errors%'
    OR vchMessage LIKE '%No user action is required%'
    OR vchMessage LIKE '%No user action required%'
    OR vchMessage LIKE '%Ce message est fourni à titre d''information. Aucune action n''est requise de la part de l''utilisateur.%' ";
            }

            sql += string.Format(@" SELECT LogDate
    , {0} AS [Log File]
    , ProcessInfo
    , RTRIM(LTRIM(vchMessage)) AS [Message]
FROM #KMOErrorLog
ORDER BY LogDate DESC

DROP TABLE #KMOErrorLog
", logFileNumber);

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get Login Failed from SQL ErrorLog
        /// </summary>
        /// <param name="s">Your smo Server</param>
        /// <param name="startTime">Doesn't return events before this datetime. You could use DateTime.MinValue if you don't want to filter.</param>
        /// <param name="endTime">Doesn't return events after this datetime. You could use DateTime.MaxValue if you don't want to filter.</param>
        /// <param name="logNumber">The file you want to parse. File 0 by default</param>
        /// <returns>a DataTable</returns>
        public static DataTable GetLoginFailed(this smo.Server s, DateTime startTime, DateTime endTime, int logFileNumber = 0)
        {
            string sql = string.Format(@"CREATE TABLE #KMOLoginFailed
(
    LogDate DATETIME
    , ProcessInfo NVARCHAR(50)
    , vchMessage NVARCHAR(2000)
)

CREATE TABLE #err_log_tmp(ArchiveNo int, CreateDate nvarchar(44), Size int)
INSERT INTO #err_log_tmp EXEC master.dbo.sp_enumerrorlogs
IF(SELECT COUNT(*) AS FileExists
FROM #err_log_tmp
WHERE ArchiveNo = {0}) = 1
BEGIN
    INSERT INTO #KMOLoginFailed(LogDate, ProcessInfo, vchMessage)
    EXEC master.dbo.xp_readerrorlog {0}
END
DROP TABLE #err_log_tmp

SELECT LogDate
    , {0} AS [Log File]
    , ProcessInfo
	, RTRIM(LTRIM(vchMessage)) AS [Message]
FROM #KMOLoginFailed
WHERE (SUBSTRING(vchMessage,1, 12) = 'Login failed'
	OR vchMessage LIKE '%SSPI%'
    OR ProcessInfo = 'Logon')
	AND LogDate > '{1}'
	AND LogDate < '{2}'
ORDER BY LogDate DESC

DROP TABLE #KMOLoginFailed", logFileNumber, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"));

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Read Database Mail Error log
        /// </summary>
        public static DataTable GetDatabaseMailErrorLog(this smo.Server s, DateTime startTime, DateTime endTime)
        {
            string sql = string.Format(@"SELECT log_date AS [Date]
    , description AS [Description]
FROM msdb.dbo.sysmail_log
WHERE event_type != 1
    AND log_date >= '{0}' 
    AND log_date < '{1}'"
, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"));

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion

        #region Queries
        /// <summary>
        /// Get the top N stored procedures more expensive
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <param name="orderQuery">With this parameter, you're able to sort the query. By default, sorted by Execution Count</param>
        /// <param name="rowCount">Number of rows returned by the query. 50 by default</param>
        /// <returns></returns>
        public static DataTable GetTop50StoredProcedures(this smo.Server s, string orderQuery = "Execution Count", int rowCount = 50)
        {
            if (s.VersionMajor < 10)
            {
                throw new Exception("This feature is only available from SQL Server 2008.");
            }

            string sql = string.Format(@"SELECT TOP ({0}) CASE WHEN database_id = 32767 THEN 'Resource' ELSE DB_NAME(database_id) END AS [Database]
	, OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id,database_id) AS [Stored Procedure]
	, cached_time AS [Cached Time]
	, last_execution_time AS [Last Execution Time]
	, execution_count AS [Execution Count]
	, total_worker_time / execution_count AS [Average CPU]
	, total_elapsed_time / execution_count AS [Average Elapsed Time]
	, total_logical_reads / execution_count AS [Average Logical Reads]
	, total_logical_writes / execution_count AS [Average Logical Writes]
	, total_physical_reads  / execution_count AS [Average Physical Reads]
FROM sys.dm_exec_procedure_stats (NOLOCK)
ORDER BY {1} DESC", rowCount, orderQuery);

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get the top N queries more expensive
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <param name="orderQuery">With this parameter, you're able to sort the query. By default, sorted by Execution Count</param>
        /// <param name="rowCount">Number of rows returned by the query. 50 by default</param>
        /// <returns></returns>
        public static DataTable GetTop50Queries(this smo.Server s, string orderQuery = "Execution Count", int rowCount = 50)
        {
            string sql = string.Format(@"SELECT TOP ({0}) CASE WHEN qp.dbid = 32767 THEN 'Resource' ELSE DB_NAME(qp.dbid) END AS [Database]
	, SUBSTRING(qt.text, qs.statement_start_offset / 2 + 1, (CASE WHEN qs.statement_end_offset = -1 THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2  ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) AS Query
	, execution_count AS [Execution Count]
	, total_worker_time/execution_count/1000 AS [Average Worker Time]
	, total_physical_reads/execution_count AS [Average Physical Reads]
	, total_logical_reads/execution_count AS [Average Logical Reads]
	, total_logical_writes/execution_count AS [Average Logical Writes]
	, total_elapsed_time/execution_count/1000 AS [Average Elapsed Time]
	, qs.last_execution_time AS [Last Execution]
	, qs.creation_time AS [Creation Date]
	, qt.text AS [Parent Query]
FROM sys.dm_exec_query_stats AS qs
	CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
	CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
ORDER BY {1} DESC", rowCount, orderQuery);

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #endregion

        #region TempDB
        /// <summary>
        /// Get list of tempdb files + io statistics
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <returns>a datatable</returns>
        public static DataTable GetTempDBFiles(this smo.Server s)
        {
            string sql = @"SELECT DB_NAME(fs.database_id) AS [Database Name]
	, df.name AS [Logical Name]
    , df.physical_name AS [Physical Name]
	, type_desc
	, CAST(size AS BIGINT) / 128 AS [Size]
	, CASE WHEN is_percent_growth = 1 THEN max_size ELSE CASE WHEN max_size = -1 THEN CAST(max_size AS BIGINT) ELSE CAST(max_size AS BIGINT) * 1024 / 128 END END AS [Max Size]
	, CASE WHEN is_percent_growth = 1 THEN CAST(growth AS BIGINT) ELSE CAST(growth AS BIGINT) * 1024 / 128 END AS [Growth]
	, is_percent_growth AS [Is Percent Growth]
	, CASE WHEN num_of_reads = 0 THEN 0
		ELSE CAST(io_stall_read_ms/num_of_reads AS NUMERIC(10,1))
		END AS [Avg Read latency ms]
	, CASE WHEN num_of_writes = 0 THEN 0
		ELSE CAST(io_stall_write_ms/num_of_writes AS NUMERIC(10,1))
		END AS [Avg Write latency ms]
	, CASE WHEN (num_of_reads + num_of_writes) = 0 THEN 0
		ELSE CAST((io_stall_read_ms + io_stall_write_ms)/(num_of_reads + num_of_writes) AS NUMERIC(10,1))
		END AS [Avg IO latency ms]
FROM sys.dm_io_virtual_file_stats(null,null) AS fs
	INNER JOIN sys.database_files AS df (NOLOCK) ON fs.database_id = DB_ID()
		AND fs.[file_id] = df.[file_id]";
            smo.Database d = s.Databases["tempdb"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get space usage from tempdb
        /// This method is inspired by the session of David Barbarin https://youtu.be/7yZ23zy80zU
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <returns>a datatable</returns>
        public static DataTable GetTempDBSpaceUsage(this smo.Server s)
        {
            string sql = @"SELECT SUM(user_object_reserved_page_count) / 128.0 AS [User Object pages in MB]
	, SUM(internal_object_reserved_page_count) / 128.0 AS [Internal object pages in MB]
	, SUM(unallocated_extent_page_count) / 128.0 AS [Free space in MB]
FROM tempdb.sys.dm_db_file_space_usage";
            smo.Database d = s.Databases["tempdb"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get task space usage in tempdb from the DMV dm_db_task_space_usage.
        /// With this method, you can find which query is using tempdb
        /// This method is inspired by the session of David Barbarin https://youtu.be/7yZ23zy80zU
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <returns>a datatable</returns>
        public static DataTable GetTempDBTaskSpaceUsage(this smo.Server s)
        {
            string sql = @"WITH all_request_usage
AS
(
	SELECT session_id
		, request_id
		, SUM(internal_objects_alloc_page_count) AS request_internal_objects_alloc_page_count
		, SUM(internal_objects_dealloc_page_count) AS request_internal_objects_dealloc_page_count
		, SUM(user_objects_alloc_page_count) AS request_user_objects_alloc_page_count
		, SUM(user_objects_dealloc_page_count) AS request_user_objects_dealloc_page_count
	FROM tempdb.sys.dm_db_task_space_usage
	WHERE session_id >= 50
		AND session_id <> @@SPID
	GROUP BY session_id
		, request_id
	HAVING (SUM(internal_objects_alloc_page_count)
		+ SUM(internal_objects_dealloc_page_count)
		+ SUM(user_objects_alloc_page_count)
		+ SUM(user_objects_dealloc_page_count)) > 0
)
SELECT R1.session_id AS [Session Id]
	, R1.request_internal_objects_alloc_page_count AS [Request Internal Objects Alloc Page Count]
	, R1.request_internal_objects_dealloc_page_count AS [Request Internal Objects Dealloc Page Count]
	, R1.request_user_objects_alloc_page_count AS [Request User Objects Alloc Page Count]
	, R1.request_user_objects_dealloc_page_count AS [Request User Objects Dealloc Page Count]
	, T.text AS [Query]
FROM all_request_usage R1
	INNER JOIN tempdb.sys.dm_exec_requests R2 ON R1.session_id = R2.session_id
		AND R1.request_id = R2.request_id
	OUTER APPLY tempdb.sys.dm_exec_sql_text(R2.sql_handle) T";
            smo.Database d = s.Databases["tempdb"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// From the default trace, get the last Sort Warning in tempdb
        /// This method is inspired by the session of David Barbarin https://youtu.be/7yZ23zy80zU
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <returns>a datatable</returns>
        public static DataTable GetTempDBSortWarning(this smo.Server s)
        {
            string sql = @"DECLARE @path NVARCHAR(520)
SELECT @path = path FROM sys.traces WHERE is_default = 1
IF(@path IS NOT NULL)
BEGIN
    SELECT TOP 1000 t.StartTime AS [Start Time]
        , v.subclass_name AS [Subclass Name]
	    , t.DatabaseName AS [Database]
	    , t.LoginName
	    , t.ApplicationName AS [Application Name]
    FROM fn_trace_gettable(@path, default) t
	    INNER JOIN sys.trace_events te ON t.EventClass = te.trace_event_id
	    INNER JOIN sys.trace_subclass_values v ON t.EventSubClass = v.subclass_value
		    AND t.EventClass = v.trace_event_id
    WHERE te.name = 'Sort Warnings'
    ORDER BY t.StartTime DESC
END
ELSE
BEGIN
    SELECT 'The default trace is disabled. You should consider to activate it.'
END";
            smo.Database d = s.Databases["tempdb"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #endregion

        #region Dashboard
        /// <summary>
        /// Get list of databases which are not backup for a while
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="checkSystemDb">True if you want to check system database backup</param>
        /// <param name="fromDays">Last backup must be done before this number of day</param>
        /// <param name="fromHours">Last backup must be done before this number of hour</param>
        /// <param name="fromMinutes">Last backup must be done before this number of minute</param>
        /// <returns></returns>
        public static DataTable DashboardBackup(this smo.Server s, bool checkSystemDb = false, int fromDays = 1, int fromHours = 0, int fromMinutes = 0)
        {
            smo.Database d = s.Databases["master"];
            string filterDb = " WHERE sdb.name NOT IN ('master', 'tempdb', 'model', 'msdb') ";
            if (checkSystemDb)
                filterDb = " WHERE sdb.name != 'tempdb' ";
            string sql = string.Format(@"SELECT DatabaseName
    , LastBackUpTime
FROM
(
	SELECT TOP 30 sdb.name AS DatabaseName
		, MAX(bus.backup_finish_date) AS LastBackUpTime
	FROM sys.databases sdb (nolock)
		LEFT OUTER JOIN msdb.dbo.backupset bus (nolock) ON bus.database_name = sdb.name AND bus.is_snapshot !=1
    {0}
    AND sdb.state != 6
    GROUP BY sdb.name
    ORDER BY LastBackUpTime, DatabaseName
)a
WHERE a.LastBackUpTime IS NULL
	OR a.LastBackUpTime < dateadd(DAY, {1}, dateadd(HOUR, {2}, dateadd(MINUTE, {3}, GETDATE())))", filterDb, fromDays, fromHours, fromMinutes);
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get files list with space
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>the result of the query</returns>
        public static DataTable DashboardFiles(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"CREATE TABLE #TMPSPACEUSED
(
	dbname NVARCHAR(128),
	filename NVARCHAR(128),
	spaceused FLOAT
)

INSERT INTO #TMPSPACEUSED
EXEC( 'sp_MSforeachdb''use [?]; SELECT ''''?'''' dbname, name AS filename,
FILEPROPERTY(name,''''spaceused'''') spaceused FROM sysfiles''')

SELECT CASE b.type WHEN 0 THEN 'DATA' ELSE b.type_desc END AS filetype
	, b.name + ' (' + a.name + ')' AS logicalfile
	, CAST((b.size * 8 / 1024.0) AS DECIMAL(18,2)) AS filesize
	, CAST((d.spaceused / 128.0) AS DECIMAL(15,2)) AS spaceused
FROM sys.databases a
	INNER JOIN sys.master_files b ON a.database_id = b.database_id
	INNER JOIN #TMPSPACEUSED d  ON a.name = d.dbname AND b.name = d.filename
WHERE a.name != 'model'

DROP TABLE #TMPSPACEUSED";
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get disk space. If Ole Automation Procedure is enable, disk space in percent else disk space in MB
        /// </summary>
        /// <param name="s">Your smo server</param>
        /// <returns>DataTable</returns>
        public static DataTable DashboardDisk(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"DECLARE @t table(drive VARCHAR(2), TotalSize BIGINT DEFAULT 0, freespace BIGINT)
INSERT INTO @t(drive, freespace) EXEC xp_fixeddrives
SELECT drive
	, TotalSize
	, freespace
FROM @t";
            if (s.IsOleAutomationProcedureActivated())
            {
                sql = @"SET NOCOUNT ON
DECLARE @hr INT
DECLARE @fso INT
DECLARE @drive CHAR(1)
DECLARE @odrive INT
DECLARE @TotalSize VARCHAR(20)
DECLARE @MB Numeric
SET @MB = 1048576
CREATE TABLE #drives (drive CHAR(1) PRIMARY KEY, freespace INT NULL, TotalSize INT NULL)
INSERT #drives(drive,freespace)
EXEC master.dbo.xp_fixeddrives
EXEC @hr=sp_OACreate 'Scripting.FileSystemObject',@fso OUT
IF @hr <> 0
	EXEC sp_OAGetErrorInfo @fso
DECLARE dcur CURSOR LOCAL FAST_FORWARD
FOR SELECT drive FROM #drives ORDER BY drive
OPEN dcur FETCH NEXT FROM dcur INTO @drive
WHILE @@FETCH_STATUS=0
BEGIN
EXEC @hr = sp_OAMethod @fso,'GetDrive', @odrive OUT, @drive
IF @hr <> 0
	EXEC sp_OAGetErrorInfo @fso
EXEC @hr = sp_OAGetProperty @odrive,'TotalSize', @TotalSize OUT
IF @hr <> 0
	EXEC sp_OAGetErrorInfo @odrive
UPDATE #drives
SET TotalSize=@TotalSize/@MB
WHERE drive=@drive
FETCH NEXT FROM dcur INTO @drive
END
CLOSE dcur
DEALLOCATE dcur
EXEC @hr=sp_OADestroy @fso
IF @hr <> 0
	EXEC sp_OAGetErrorInfo @fso
SELECT drive
	, TotalSize, freespace
FROM #drives
ORDER BY drive
DROP TABLE #drives";
            }
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Just a simple query to test the connectivity
        /// </summary>
        /// <param name="s">your smo server object</param>
        /// <returns>a datatable</returns>
        public static DataTable DashboardIsAlive(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults("select 1").Tables[0];
        }

        public static DataTable DashboardGetLocks(this smo.Server s, int durationInMinute, List<string> queriesToIgnore)
        {
            StringBuilder filter = new StringBuilder();
            foreach (string query in queriesToIgnore)
            {
                filter.AppendLine(" AND st.text NOT LIKE '" + query + "'");
            }

            smo.Database d = s.Databases["master"];
            string sql = string.Format(@"SELECT TOP 20 qe.session_id
	, qe.blocking_session_id
	, qe.start_time
	, CASE WHEN LEN(st.text) < 100 THEN st.text ELSE SUBSTRING(st.text, 0, 100) + ' (...)' END AS query
FROM sys.dm_exec_requests qe (nolock)
	INNER JOIN sys.dm_exec_sessions s (nolock) ON qe.session_id = s.session_id
	CROSS APPLY sys.dm_exec_sql_text(qe.sql_handle) st
WHERE (DATEDIFF(SECOND, qe.start_time, GETDATE())) / {0} > 0
	AND s.is_user_process = 1
    AND st.text NOT LIKE 'sp_server_diagnostics%'
    {1}
ORDER BY qe.start_time", durationInMinute, filter.ToString());
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get the actual Page Life Expectancy
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DataTable DashboardPle(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"CREATE TABLE #MSVER(ID INT, Name SYSNAME, Internal_Value INT, Value NVARCHAR(512))
INSERT INTO #MSVER EXEC master.dbo.xp_msver
SELECT opc.cntr_value as PLE
	, CASE WHEN c.value_in_use < t.Internal_Value THEN c.value_in_use ELSE t.Internal_Value END AS value_in_use
FROM sys.dm_os_performance_counters opc (NOLOCK)
    CROSS APPLY sys.configurations c
    CROSS APPLY #MSVER t
WHERE object_name LIKE '%Manager%'
	AND counter_name = 'Page life expectancy'
	AND c.name LIKE 'max server memory%'
	AND t.Name = N'PhysicalMemory'
DROP TABLE #MSVER";
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get the sql and system cpu average of the last 30 minutes
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DataTable DashboardCpu(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            const string sql = @"DECLARE @ts_now bigint
SELECT @ts_now = cpu_ticks / (cpu_ticks / ms_ticks)
FROM sys.dm_os_sys_info
;WITH ring AS
(
    SELECT
       record.value('(Record/@id)[1]', 'int') AS record_id,
       DATEADD (ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS EventTime,
       100 - record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_post_sp2,
       record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_post_sp2 ,
       100 - record.value('(Record/SchedluerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS system_cpu_utilization_pre_sp2,
       record.value('(Record/SchedluerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sql_cpu_utilization_pre_sp2
     FROM (
       SELECT TOP 30 timestamp, CONVERT (xml, record) AS record
       FROM sys.dm_os_ring_buffers
       WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
         AND record LIKE '%<SystemHealth>%'
	   ORDER BY timestamp DESC) AS t
), cte AS
(
    SELECT EventTime
        , CASE WHEN system_cpu_utilization_post_sp2 IS NOT NULL THEN system_cpu_utilization_post_sp2 ELSE system_cpu_utilization_pre_sp2 END AS system_cpu
        , CASE WHEN sql_cpu_utilization_post_sp2 IS NOT NULL THEN sql_cpu_utilization_post_sp2 ELSE sql_cpu_utilization_pre_sp2 END AS sql_cpu
    FROM ring
)
SELECT AVG(system_cpu) AS system_cpu
    , AVG(CASE WHEN sql_cpu > system_cpu THEN sql_cpu / 2 ELSE sql_cpu END) AS cpu
FROM cte";
            return d.ExecuteWithResults(sql).Tables[0];
        }


        public static DataTable DashboardJob(this smo.Server s, DateTime logDay, List<string> errorToIgnore)
        {
            StringBuilder sqlErrorToIgnore = new StringBuilder();
            foreach (string sqlError in errorToIgnore)
            {
                sqlErrorToIgnore.Append(" AND h.message NOT LIKE '" + sqlError + "'");
            }

            smo.Database d = s.Databases["msdb"];
            string sql = string.Format(@"SELECT j.name
    , h.message
    , h.run_date
    , h.run_time
FROM msdb.dbo.sysjobs j (NOLOCK)
    INNER JOIN msdb.dbo.sysjobhistory h (NOLOCK) ON h.job_id = j.job_id
WHERE run_date = {0}
    AND run_status = 0
{1}
ORDER BY run_date desc
    , run_time desc", logDay.ToString("yyyyMMdd"), sqlErrorToIgnore.ToString());
            return d.ExecuteWithResults(sql).Tables[0];
        }

        public static DataTable DashboardErrorLog(this smo.Server s, TimeSpan logFrom, bool withReplicationLog, bool withInformationMessage, bool withDatabaseMail, List<string> errorToIgnore)
        {
            string sql = string.Format(@"
DECLARE @sqlStatement VARCHAR(1000);
SET @sqlStatement = 'master.dbo.xp_readerrorlog'
DECLARE @REPLICATION TABLE (LogDate DATETIME,ProcessInfo NVARCHAR(50),vchMessage NVARCHAR(2000))
DECLARE @Errors TABLE (LogDate DATETIME,ProcessInfo NVARCHAR(50),vchMessage NVARCHAR(2000))
INSERT @Errors(LogDate, ProcessInfo, vchMessage) EXEC @sqlStatement

DELETE FROM @Errors
WHERE LogDate < DATEADD(SECOND, -{0}, DATEADD(MINUTE, -{1}, DATEADD(HH, -{2}, DATEADD(DAY, -{3}, GETDATE()))))
",
                logFrom.Seconds, logFrom.Minutes, logFrom.Hours, logFrom.Days);

            StringBuilder sqlErrorToIgnore = new StringBuilder();
            foreach (string sqlError in errorToIgnore)
            {
                sqlErrorToIgnore.AppendLine(string.Format(" delete FROM @Errors where vchMessage like '{0}' ", sqlError));
            }
            sql += sqlErrorToIgnore.ToString();

            if (withReplicationLog)
            {
                sql += string.Format(@"
DECLARE @DatabaseName NVARCHAR(128)
DECLARE @SQLScript VARCHAR(8000)

DECLARE DatabaseCursor CURSOR FOR
	SELECT name
	FROM sys.databases
	WHERE is_distributor = 1
OPEN DatabaseCursor
FETCH NEXT FROM DatabaseCursor INTO @DatabaseName
WHILE @@FETCH_STATUS = 0
BEGIN
	SET @SQLScript = 'USE [' + @DatabaseName + '];
		IF EXISTS(SELECT * FROM sys.objects WHERE name = ''MSrepl_errors'')
		BEGIN
			SELECT [time] AS LogDate
				, ''''
				, DB_NAME() + '' '' + source_name + '' : '' + CAST(error_text AS NVARCHAR(4000)) AS texte
			FROM dbo.MSrepl_errors WITH (READUNCOMMITTED)
			WHERE [time] >= dateadd(SECOND, -{0}, dateadd(MINUTE, -{1}, dateadd(HH, -{2}, dateadd(DAY, -{3}, GETDATE()))))
            ORDER BY time DESC
		END'
	INSERT INTO @REPLICATION (LogDate, ProcessInfo, vchMessage)
	EXEC (@SQLScript)
	FETCH NEXT FROM DatabaseCursor INTO @DatabaseName
END
CLOSE DatabaseCursor
DEALLOCATE DatabaseCursor
", logFrom.Seconds, logFrom.Minutes, logFrom.Hours, logFrom.Days);
            }

            sql += @"
SELECT LogDate AS LogDate
    , RTRIM(LTRIM(vchMessage)) AS texte
    , ProcessInfo
FROM @Errors";

            if (!withInformationMessage)
            {
                sql += @" WHERE vchMessage NOT LIKE '%This is an informational message%'
AND vchMessage NOT LIKE '%Ce message est fourni à titre d''information. Aucune action n''est requise de la part de l''utilisateur.%'
AND vchMessage NOT LIKE '%DBCC CHECKDB%found 0 errors and repaired 0 errors%'
AND vchMessage NOT LIKE '%No user action is required%'
AND vchMessage NOT LIKE '%No user action required%' ";
            }

            if (withDatabaseMail)
            {
                sql += string.Format(@"
UNION ALL
SELECT log_date AS LogDate
    , '[DBMAIL] ' + description AS texte
    , ''
FROM msdb.dbo.sysmail_log
WHERE event_type != 1
    AND log_date >= dateadd(SECOND, -{0}, dateadd(MINUTE, -{1}, dateadd(HH, -{2}, dateadd(DAY, -{3}, GETDATE()))))",
                    logFrom.Seconds, logFrom.Minutes, logFrom.Hours, logFrom.Days);
            }

            if (withReplicationLog)
            {
                sql += @"
UNION ALL
SELECT LogDate AS LogDate
    , '[REPLICATION] ' + vchMessage AS texte
    , ProcessInfo
FROM @REPLICATION";
            }

            sql += @"
ORDER BY LogDate DESC";
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get the statistics used in Dashboard Instance
        /// Statistics of the last 1 second
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static DataTable DashboardInstance(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            const string sql = @"DECLARE @BATCHREQUESTPERSECOND BIGINT
DECLARE @COMPILATIONPERSECOND BIGINT
DECLARE @RECOMPILATIONPERSECOND BIGINT
DECLARE @LOCKWAITPERSECOND BIGINT
DECLARE @PAGESPLITPERSECOND BIGINT
DECLARE @CHECKPOINTPAGEPERSECOND BIGINT
DECLARE @LAZYWRITEPERSECOND BIGINT
DECLARE @TRANSACTIONPERSECOND BIGINT
DECLARE @IOSTALLREADPERSECOND BIGINT
DECLARE @IOSTALLWRITEPERSECOND BIGINT
DECLARE @DATEPOINT DATETIME

DECLARE @STATSBEFORE TABLE ([object_name] VARCHAR (128)
	, [counter_name] VARCHAR (128)
	, [instance_name] VARCHAR (128)
	, [cntr_value] BIGINT
	, [cntr_type] INT)

DECLARE @STATSAFTER TABLE ([object_name] VARCHAR (128)
	, [counter_name] VARCHAR (128)
	, [instance_name] VARCHAR (128)
	, [cntr_value] BIGINT
	, [cntr_type] INT)

SET @DATEPOINT = GETDATE()

INSERT INTO @STATSBEFORE ([object_name]
			, [counter_name]
			, [instance_name]
			, [cntr_value]
			, [cntr_type])
SELECT [object_name]
	, [counter_name]
	, [instance_name]
	, [cntr_value]
	, [cntr_type]
FROM sys.dm_os_performance_counters
WHERE (counter_name = 'Buffer cache hit ratio'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name = 'Buffer cache hit ratio base'
			AND object_name LIKE '%Buffer Banager%')
	OR (counter_name = 'Page life expectancy '
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name = 'Batch Requests/sec'
			AND object_name LIKE '%SQL Statistics%')
	OR (counter_name = 'SQL Compilations/sec'
			AND object_name LIKE '%SQL Statistics%')
	OR (counter_name = 'SQL Re-Compilations/sec'
			AND object_name LIKE '%SQL Statistics%')
	OR (counter_name = 'User Connections'
			AND object_name LIKE '%General Statistics%')
	OR (counter_name = 'Lock Waits/sec'
			AND instance_name = '_Total'
			AND object_name LIKE '%Locks%')
	OR (counter_name = 'Page Splits/sec'
			AND object_name LIKE '%Access Methods%')
	OR (counter_name = 'Processes blocked'
			AND object_name LIKE '%General Statistics%')
	OR (counter_name = 'Checkpoint pages/sec'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name LIKE 'Lazy writes/sec%'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name LIKE 'Transactions/sec%'
			AND instance_name LIKE '_Total%')

SELECT TOP 1 @BATCHREQUESTPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name = 'Batch Requests/sec'
	AND object_name LIKE '%SQL Statistics%'

SELECT TOP 1 @COMPILATIONPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name = 'SQL Compilations/sec'
	AND object_name LIKE '%SQL Statistics%'

SELECT TOP 1 @RECOMPILATIONPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name = 'SQL Re-Compilations/sec'
	AND object_name LIKE '%SQL Statistics%'

SELECT TOP 1 @LOCKWAITPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name = 'Lock Waits/sec'
	AND instance_name = '_Total'
	AND object_name LIKE '%Locks%'

SELECT TOP 1 @PAGESPLITPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name = 'Page Splits/sec'
	AND object_name LIKE '%Access Methods%'

SELECT TOP 1 @CHECKPOINTPAGEPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name = 'Checkpoint pages/sec'
	AND object_name LIKE '%Buffer Manager%'

SELECT TOP 1 @LAZYWRITEPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name LIKE 'Lazy writes/sec%'
	AND object_name LIKE '%Buffer Manager%'

SELECT TOP 1 @TRANSACTIONPERSECOND = cntr_value
FROM @STATSBEFORE
WHERE counter_name LIKE 'Transactions/sec%'
	AND instance_name LIKE '_Total%'

SELECT @IOSTALLREADPERSECOND = SUM(io_stall_read_ms)
	, @IOSTALLWRITEPERSECOND = SUM(io_stall_write_ms)
FROM sys.dm_io_virtual_file_stats(NULL, NULL)


WAITFOR DELAY '00:00:01'

INSERT INTO @STATSAFTER ([object_name]
	, [counter_name]
	, [instance_name]
	, [cntr_value]
	, [cntr_type])

SELECT [object_name]
	, [counter_name]
	, [instance_name]
	, [cntr_value]
	, [cntr_type]
FROM sys.dm_os_performance_counters
WHERE (counter_name = 'Buffer cache hit ratio'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name = 'Buffer cache hit ratio base'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name = 'Page life expectancy '
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name = 'Batch Requests/sec'
			AND object_name LIKE '%SQL Statistics%')
	OR (counter_name = 'SQL Compilations/sec'
			AND object_name LIKE '%SQL Statistics%')
	OR (counter_name = 'SQL Re-Compilations/sec'
			AND object_name LIKE '%SQL Statistics%')
	OR (counter_name = 'User Connections'
			AND object_name LIKE '%General Statistics%')
	OR (counter_name = 'Lock Waits/sec'
			AND instance_name = '_Total'
			AND object_name LIKE '%Locks%')
	OR (counter_name = 'Page Splits/sec'
			AND object_name LIKE '%Access Methods%')
	OR (counter_name = 'Processes blocked'
			AND object_name LIKE '%General Statistics%')
	OR (counter_name = 'Checkpoint pages/sec'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name LIKE 'Lazy writes/sec%'
			AND object_name LIKE '%Buffer Manager%')
	OR (counter_name LIKE 'Transactions/sec%'
			AND instance_name LIKE '_Total%')

SELECT FLOOR(ROUND(a.cntr_value * 1.0 / b.cntr_value * 100, 0)) [buffercachehitratio]
	, c.cntr_value AS [pagelifeexpectency]
	, d.[batchrequestspersecond]
	, e.[compilationspersecond]
	, f.[recompilationspersecond]
	, g.cntr_value AS [userconnections]
	, h.lockwaitspersecond
	, i.pagesplitspersecond
	, j.cntr_value AS [processesblocked]
	, k.checkpointpagespersecond
	, l.lazywriterpersecond
	, m.transactionpersecond
	, n.iostallreadpersecond
	, n.iostallwritepersecond
	, GETDATE() AS statdate
FROM (
		SELECT *
		FROM @STATSAFTER
		WHERE counter_name = 'Buffer cache hit ratio'
			AND object_name LIKE '%Buffer Manager%') a
	CROSS JOIN (
		SELECT *
		FROM @STATSAFTER
		WHERE counter_name = 'Buffer cache hit ratio base'
			AND object_name LIKE '%Buffer Manager%') b
	CROSS JOIN (
		SELECT *
		FROM @STATSAFTER
		WHERE counter_name = 'Page life expectancy '
			AND object_name LIKE '%Buffer Manager%') c
	CROSS JOIN (
		SELECT (cntr_value - @BATCHREQUESTPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [batchrequestspersecond]
		FROM @STATSAFTER
		WHERE counter_name = 'Batch Requests/sec'
			AND object_name LIKE '%SQL Statistics%') d
	CROSS JOIN (
		SELECT (cntr_value - @COMPILATIONPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [compilationspersecond]
		FROM @STATSAFTER
		WHERE counter_name = 'SQL Compilations/sec'
			AND object_name LIKE '%SQL Statistics%') e
	CROSS JOIN (
		SELECT (cntr_value - @RECOMPILATIONPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [recompilationspersecond]
		FROM @STATSAFTER
		WHERE counter_name = 'SQL Re-Compilations/sec'
			AND object_name LIKE '%SQL Statistics%') f
	CROSS JOIN (
		SELECT *
		FROM @STATSAFTER
		WHERE counter_name = 'User Connections'
			AND object_name LIKE '%General Statistics%') g
	CROSS JOIN (
		SELECT (cntr_value - @LOCKWAITPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [lockwaitspersecond]
		FROM @STATSAFTER
		WHERE counter_name = 'Lock Waits/sec'
			AND instance_name = '_Total'
			AND object_name LIKE '%Locks%') h
	CROSS JOIN (
		SELECT (cntr_value - @PAGESPLITPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [pagesplitspersecond]
		FROM @STATSAFTER
		WHERE counter_name = 'Page Splits/sec'
			AND object_name LIKE '%Access Methods%') i
	CROSS JOIN (
		SELECT *
		FROM @STATSAFTER
		WHERE counter_name = 'Processes blocked'
			AND object_name LIKE '%General Statistics%') j
	CROSS JOIN (
		SELECT (cntr_value - @CHECKPOINTPAGEPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [checkpointpagespersecond]
		FROM @STATSAFTER
		WHERE counter_name = 'Checkpoint pages/sec'
			AND object_name LIKE '%Buffer Manager%') k
	CROSS JOIN (
		SELECT (cntr_value - @LAZYWRITEPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [lazywriterpersecond]
		FROM @STATSAFTER
		WHERE counter_name LIKE 'Lazy writes/sec%'
			AND object_name LIKE '%Buffer Manager%') l
	CROSS JOIN (
		SELECT (cntr_value - @TRANSACTIONPERSECOND) / (CASE WHEN DATEDIFF (ss , @DATEPOINT , GETDATE()) = 0 THEN 1
				ELSE DATEDIFF (ss , @DATEPOINT , GETDATE()) END) AS [transactionpersecond]
		FROM @STATSAFTER
		WHERE counter_name LIKE 'Transactions/sec%'
			AND instance_name LIKE '_Total%') m
	CROSS JOIN (
		SELECT SUM(io_stall_read_ms) - @IOSTALLREADPERSECOND AS [iostallreadpersecond]
			, SUM(io_stall_write_ms) - @IOSTALLWRITEPERSECOND AS [iostallwritepersecond]
		FROM sys.dm_io_virtual_file_stats(null, null)) n";
            return d.ExecuteWithResults(sql).Tables[0];
        }


        public static bool IsOleAutomationProcedureActivated(this smo.Server s)
        {
            foreach (Microsoft.SqlServer.Management.Smo.ConfigProperty c in s.Configuration.Properties)
            {
                if (c.DisplayName == "Ole Automation Procedures" && c.RunValue == 1)
                {
                    return true;
                }
            }
            return false;
        }
        #endregion

        #region Latches
        /// <summary>
        /// Get latch statistics for an instance. Used in Live Latches Profiler
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>return the result of the query in a DataTable</returns>
        public static DataTable GetLiveLatchesProfiler(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            const string sql = @"SELECT latch_class AS [Latch]
	, CASE WHEN CHARINDEX('_', latch_class, 0) != 0 THEN SUBSTRING(latch_class,0, CHARINDEX('_', latch_class, 0)) ELSE latch_class END AS [Category]
	, wait_time_ms AS [Time]
	, waiting_requests_count AS [Count]
	, max_wait_time_ms AS [MaxTime]
FROM sys.dm_os_latch_stats";
            DataTable dt = d.ExecuteWithResults(sql).Tables[0];
            dt.Columns.Add(new DataColumn("Description"));
            foreach (DataRow dr in dt.Rows)
            {
                string latch = dr["Latch"].ToString();
                dr["Description"] = (from l in Resources.Latches.GetLatchesDictionary()
                                     where l.Key == latch
                                     select l.Value).SingleOrDefault();
            }
            return dt;
        }
        #endregion

        #region Dashboard Availability Groups

        /// <summary>
        /// Get availability groups without distributed groups
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <returns>a DataTable</returns>
        public static DataTable DashboardAG_GetAvailabilityGroups(this smo.Server s)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT group_id as groupId
	, name as groupName
FROM sys.availability_groups
ORDER BY groupName";
            if (s.VersionMajor >= 13)
            {
                sql = @"SELECT group_id AS groupId
	, name AS groupName
FROM sys.availability_groups
WHERE is_distributed = 0
ORDER BY groupName";
            }
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get database list for a given availability group
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="groupId">availability group id. UniqueIdentifier</param>
        /// <returns>a DataTable</returns>
        public static DataTable DashboardAG_GetDatabases(this smo.Server s, string groupId)
        {
            smo.Database d = s.Databases["master"];
            string sql = String.Format(@"SELECT database_name AS dbName
FROM sys.availability_databases_cluster
WHERE group_id = '{0}'
ORDER BY dbName", groupId);
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get the replica list and its role for an availability group
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="groupId">availability group id. UniqueIdentifier</param>
        /// <returns>a DataTable</returns>
        public static DataTable DashboardAG_GetReplica(this smo.Server s, string groupId)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT r.replica_server_name AS hostName
    , s.role AS isPrimary
FROM sys.availability_replicas r
    INNER JOIN sys.dm_hadr_availability_replica_states s ON r.group_id = s.group_id
        AND r.replica_id = s.replica_id
WHERE r.group_id = '{0}'
ORDER BY hostName";

            if (s.VersionMajor >= 13)
            {
                sql = @"WITH dist AS
(
    SELECT g.group_id AS distributedGroupId
        , g.name
        , d.group_id AS localGroupId
    FROM sys.availability_groups g
        INNER JOIN sys.availability_replicas r ON g.group_id = r.group_id
        CROSS APPLY sys.fn_hadr_distributed_ag_replica(g.group_id, r.replica_id) d
    WHERE is_distributed = 1
        AND d.group_id = '{0}'
)
SELECT r.replica_server_name AS hostName
    , s.role as isPrimary
    , r.group_id
FROM sys.availability_replicas r
    INNER JOIN sys.dm_hadr_availability_replica_states s ON r.group_id = s.group_id
        AND r.replica_id = s.replica_id
WHERE r.group_id = '{0}'
UNION ALL
SELECT r.replica_server_name as hostName
    , s.role as isPrimary
    , r.group_id
FROM sys.availability_replicas r
    INNER JOIN dist ON r.group_id = dist.distributedGroupId
    INNER JOIN sys.dm_hadr_availability_replica_states s ON r.group_id = s.group_id
        AND r.replica_id = s.replica_id
WHERE role = 2
ORDER BY hostName";
            }
            return d.ExecuteWithResults(string.Format(sql, groupId)).Tables[0];
        }

        /// <summary>
        /// Get the health state for each replica of an availability group
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="groupId">availability group id. UniqueIdentifier</param>
        /// <returns>a DataTable</returns>
        public static DataTable DashboardAG_GetReplicasInfo(this smo.Server s, string groupId)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT g.name
    , rs.role
    , gs.synchronization_health_desc AS groupstate
    , r.replica_id
    , r.replica_server_name
    , r.availability_mode
    , rs.operational_state
    , rs.operational_state_desc
    , rs.synchronization_health
    , rs.synchronization_health_desc
FROM sys.availability_groups g
    INNER JOIN sys.dm_hadr_availability_group_states gs ON g.group_id = gs.group_id
    INNER JOIN sys.availability_replicas r ON g.group_id = r.group_id
    INNER JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id = rs.replica_id
        AND r.group_id = rs.group_id
WHERE g.group_id = '{0}'
ORDER BY role
    , availability_mode desc
    , r.replica_server_name";

            if (s.VersionMajor >= 13)
            {
                sql = @"WITH dist AS
(
	SELECT g.group_id AS distributedGroupId
		, g.name
		, d.group_id AS localGroupId
	FROM sys.availability_groups g
	    INNER JOIN sys.availability_replicas r on g.group_id = r.group_id
	    CROSS APPLY sys.fn_hadr_distributed_ag_replica(g.group_id, r.replica_id) d
	WHERE is_distributed = 1
	    AND d.group_id = '{0}'
)
SELECT g.name
	, rs.role
	, gs.synchronization_health_desc as groupstate
	, r.replica_id
	, r.replica_server_name
	, r.availability_mode
	, rs.operational_state
	, rs.operational_state_desc
	, rs.synchronization_health
	, rs.synchronization_health_desc
    , 1 AS sort
FROM sys.availability_groups g
	INNER JOIN sys.dm_hadr_availability_group_states gs on g.group_id = gs.group_id
	INNER JOIN sys.availability_replicas r on g.group_id = r.group_id
	INNER JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id = rs.replica_id
        AND r.group_id = rs.group_id
WHERE g.group_id = '{0}'
UNION ALL
SELECT g.name
	, rs.role
	, gs.synchronization_health_desc as groupstate
	, r.replica_id
	, r.replica_server_name
	, 2
	, rs.operational_state
	, rs.operational_state_desc
	, rs.synchronization_health
	, rs.synchronization_health_desc
    , 2 AS sort
FROM sys.availability_groups g
	INNER JOIN sys.dm_hadr_availability_group_states gs ON g.group_id = gs.group_id
	INNER JOIN sys.availability_replicas r ON g.group_id = r.group_id
	INNER JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id = rs.replica_id
        AND r.group_id = rs.group_id
	INNER JOIN dist ON r.group_id = dist.distributedGroupId
WHERE role = 2
    AND EXISTS (SELECT * FROM sys.availability_groups g2 WHERE g2.name = gs.primary_replica)
ORDER BY sort
    , role
	, availability_mode desc
    , r.replica_server_name";
            }
            return d.ExecuteWithResults(string.Format(sql, groupId)).Tables[0];
        }

        /// <summary>
        /// Get health status of each replica by database
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="groupId">availability group id. UniqueIdentifier</param>
        /// <returns>a DataTable</returns>
        public static DataTable DashboardAG_GetDatabasesInfo(this smo.Server s, string groupId)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT d.group_database_id
	, r.replica_server_name
	, d.database_name
	, drs.database_state_desc
	, drs.synchronization_state_desc
	, r.availability_mode
    , drs.log_send_queue_size
    , drs.log_send_rate
    , drs.redo_queue_size
    , drs.redo_rate
FROM sys.availability_databases_cluster d
	INNER JOIN sys.dm_hadr_database_replica_states drs ON d.group_id = drs.group_id
        AND d.group_database_id = drs.group_database_id
	INNER JOIN sys.availability_replicas r ON drs.replica_id = r.replica_id
        AND drs.group_id = r.group_id
	INNER JOIN sys.dm_hadr_availability_replica_states rs ON r.replica_id = rs.replica_id
        AND r.group_id = rs.group_id
WHERE d.group_id = '{0}'
ORDER BY d.database_name
	, rs.role
	, availability_mode desc
    , r.replica_server_name";
            return d.ExecuteWithResults(string.Format(sql, groupId)).Tables[0];
        }

        /// <summary>
        /// Get the primary replica name for a given availability group
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="groupId">availability group id. UniqueIdentifier</param>
        /// <returns>a DataTable</returns>
        public static DataTable DashboardAG_GetPrimary(this smo.Server s, string groupId)
        {
            smo.Database d = s.Databases["master"];
            string sql = @"SELECT primary_replica
FROM sys.dm_hadr_availability_group_states
WHERE group_id = '{0}'";
            return d.ExecuteWithResults(string.Format(sql, groupId)).Tables[0];
        }

        #endregion

        #region Trace
        /// <summary>
        /// Read the default trace
        /// https://docs.microsoft.com/fr-fr/sql/relational-databases/system-functions/sys-fn-trace-gettable-transact-sql
        /// You need the ALTER TRACE permission on the server
        /// </summary>
        /// <param name="s">the smo server</param>
        /// <param name="startTime">Start date filter</param>
        /// <param name="endTime">End date filter</param>
        /// <param name="topRows">Maximum row count</param>
        /// <param name="category">filter by category. Category ID from sys.trace_categories</param>
        public static DataTable ReadDefaultTrace(this smo.Server s, DateTime startTime, DateTime endTime, int topRows = 1000, int category = 0)
        {
            string categoryFilter = string.Empty;
            if(category != 0)
            {
                categoryFilter = string.Format(" AND c.category_id = {0}", category);
            }
            string sql = string.Format(@"DECLARE @PATH NVARCHAR(1000)
SELECT @PATH = SUBSTRING(path, 1, LEN(path) - CHARINDEX('\', REVERSE(path))) + '\log.trc'
FROM sys.traces
WHERE is_default = 1;

IF @PATH IS NOT NULL
BEGIN
    SELECT TOP ({0}) t.StartTime AS [Start Time]
	    , t.EndTime AS [End Time]
	    , c.name AS [Category]
	    , e.name AS [Event Class]
	    , v.subclass_name AS [Sub Class]
	    , t.LoginName AS [Login]
	    , t.HostName AS [Hostname]
	    , t.ApplicationName AS [Application Name]
	    , t.DatabaseName AS [Database]
	    , t.ObjectName AS [Object]
	    , t.TextData AS [Text Data]
    FROM ::fn_trace_gettable(@PATH, default ) t
	    INNER JOIN sys.trace_events e ON t.EventClass = e.trace_event_id
	    INNER JOIN sys.trace_categories c ON e.category_id = c.category_id
	    LEFT JOIN sys.trace_subclass_values v ON t.EventSubClass = v.subclass_value AND t.EventClass = v.trace_event_id
    WHERE StartTime BETWEEN '{1}' AND '{2}'
        AND (t.TextData IS NULL
		    OR (t.TextData NOT LIKE '%FROM ::fn_trace_gettable(@PATH, default ) t%'
	            AND t.TextData NOT LIKE '%SELECT @PATH = SUBSTRING(path, 1, LEN(path) - CHARINDEX%'))
    {3}
    ORDER BY t.StartTime DESC
	    , t.EndTime DESC
END
BEGIN
    SELECT 'The default trace is disabled.' AS [Error Message]
END"
, topRows, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"), categoryFilter);

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get trace categories from sys.trace_categories
        /// Used in the Default trace tool
        /// </summary>
        public static DataTable GetTraceCategories(this smo.Server s)
        {
            string sql = @"SELECT category_id
	, name
FROM sys.trace_categories
ORDER BY name";
            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get rowcount from default trace group by Category
        /// </summary>
        public static DataTable DefaultTraceCountByCategory(this smo.Server s, DateTime startTime, DateTime endTime)
        {
            string sql = string.Format(@"DECLARE @PATH NVARCHAR(1000)
SELECT @PATH = SUBSTRING(path, 1, LEN(path) - CHARINDEX('\', REVERSE(path))) + '\log.trc'
FROM sys.traces
WHERE is_default = 1;

IF @PATH IS NOT NULL
BEGIN
    SELECT c.name AS [Category]
        , COUNT(*) AS [Count]
    FROM ::fn_trace_gettable(@PATH, default ) t
	    INNER JOIN sys.trace_events e ON t.EventClass = e.trace_event_id
	    INNER JOIN sys.trace_categories c ON e.category_id = c.category_id
    WHERE StartTime BETWEEN '{0}' AND '{1}'
        AND (t.TextData IS NULL
		    OR (t.TextData NOT LIKE '%FROM ::fn_trace_gettable(@PATH, default ) t%'
	            AND t.TextData NOT LIKE '%SELECT @PATH = SUBSTRING(path, 1, LEN(path) - CHARINDEX%'))
    GROUP BY c.name
END
ELSE
BEGIN
    SELECT c.name AS [Category]
        , COUNT(*) AS [Count]
    FROM sys.trace_categories c
    WHERE c.category_id = -1
    GROUP BY c.name
END"
, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"));

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get rowcount from default trace group by Event
        /// </summary>
        public static DataTable DefaultTraceCountByEvent(this smo.Server s, DateTime startTime, DateTime endTime)
        {
            string sql = string.Format(@"DECLARE @PATH NVARCHAR(1000)
SELECT @PATH = SUBSTRING(path, 1, LEN(path) - CHARINDEX('\', REVERSE(path))) + '\log.trc'
FROM sys.traces
WHERE is_default = 1;

IF @PATH IS NOT NULL
BEGIN
    SELECT e.name AS [Category]
        , COUNT(*) AS [Count]
    FROM ::fn_trace_gettable(@PATH, default ) t
	    INNER JOIN sys.trace_events e ON t.EventClass = e.trace_event_id
	    INNER JOIN sys.trace_categories c ON e.category_id = c.category_id
    WHERE StartTime BETWEEN '{0}' AND '{1}'
        AND (t.TextData IS NULL
		    OR (t.TextData NOT LIKE '%FROM ::fn_trace_gettable(@PATH, default ) t%'
	            AND t.TextData NOT LIKE '%SELECT @PATH = SUBSTRING(path, 1, LEN(path) - CHARINDEX%'))
    GROUP BY e.name
END
ELSE
BEGIN
    SELECT e.name AS [Category]
        , COUNT(*) AS [Count]
    FROM sys.trace_events e
    WHERE e.trace_event_id = -1
    GROUP BY e.name
END"
, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"));

            smo.Database d = s.Databases["master"];
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion

        #region Jobs
        /// <summary>
        /// Get the detailed job log from SQL Server Agent
        /// </summary>
        /// <param name="s">your smo server</param>
        /// <param name="startTime">job executed after this date</param>
        /// <param name="endTime">job executed before this date</param>
        /// <param name="errorOnly">If true, you won't get successful execution log</param>
        /// <returns></returns>
        public static DataTable GetJobLog(this smo.Server s, DateTime startTime, DateTime endTime, bool errorOnly)
        {
            string filterStatus = string.Empty;
            if (errorOnly)
                filterStatus = " AND h.run_status != 1 ";

            string sql = string.Format(@"WITH ctejob AS (
	SELECT j.job_id
		, cat.name AS categoryname
		, isnull(j.name, j.job_id) AS jobname
		, dbo.agent_datetime(h.run_date, h.run_time) AS agentDateTime
		, s.step_id
		, isnull(s.step_name, s.step_id) AS stepname
		, h.run_status
		, STUFF(STUFF(STUFF(RIGHT(REPLICATE('0', 8) + CAST(h.run_duration AS VARCHAR(8)), 8), 3, 0, ':'), 6, 0, ':'), 9, 0, ':') AS readableRunDuration
        , ((h.run_duration / 10000)*3600) + (((h.run_duration / 100) % 100) * 60) +( (h.run_duration) % 100) AS runDurationSecond
		, h.message
		, j.enabled AS jobenabled
        , h.retries_attempted
	FROM msdb.dbo.sysjobs j
		LEFT JOIN msdb.dbo.sysjobsteps s on j.job_id = s.job_id
		LEFT JOIN msdb.dbo.syscategories cat on j.category_id = cat.category_id
		INNER JOIN msdb.dbo.sysjobhistory h on s.job_id = h.job_id and s.step_id = h.step_id
	WHERE run_date BETWEEN {0} AND {1}
		{2}
)
SELECT jobname AS [Job Name]
	, stepname AS [Step Name]
	, agentDateTime AS [Start Run Date]
    , DATEADD(SS, runDurationSecond, agentDateTime) AS [End Run Date]
    , readableRunDuration AS [Run Duration]
    , categoryname AS [Category]
	, CASE WHEN run_status = 0 THEN 'Failed'
        WHEN run_status = 1 THEN 'Succeeded'
        WHEN run_status = 2 THEN 'Retry'
        WHEN run_status = 3 THEN 'Canceled'
        ELSE 'Unknown' END AS [Run Status]
    , message AS [Message]
	, CASE WHEN run_status = 0 THEN '#33FF0000'
        WHEN run_status = 2 THEN '#33FFCC00'
        WHEN run_status = 3 THEN '#33FF8C00' END AS [__rowColor]
FROM ctejob
WHERE agentDateTime BETWEEN '{3}' AND '{4}'
ORDER BY agentDateTime DESC
    , retries_attempted DESC"
, startTime.ToString("yyyyMMdd"), endTime.ToString("yyyyMMdd"), filterStatus, startTime.ToString("yyyyMMdd HH:mm:ss"), endTime.ToString("yyyyMMdd HH:mm:ss"));

            smo.Database d = s.Databases["msdb"];
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #endregion
    }
}
