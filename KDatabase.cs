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
        #region Replication
        /// <summary>
        /// Return true is the database is a distributor database
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a bool</returns>
        public static bool IsDistributor(this smo.Database d)
        {
            string sql = string.Format("SELECT 1 FROM sys.databases WHERE is_distributor = 1 AND name = '{0}'", d.Name);
            DataSet ds = d.ExecuteWithResults(sql);
            return (ds.Tables[0].Rows.Count == 1);
        }

        /// <summary>
        /// Get replication details (command delivered/undelivered) by publication, subscriber
        /// Must be executed on distribution db
        /// </summary>
        /// <param name="d">Your smo database (distribution)</param>
        /// <returns>a datatable</returns>
        public static DataTable GetReplicationDetails(this smo.Database d)
        {
            DataTable dt = new DataTable();
            if (d.IsDistributor())
            {
                string sql = @"SELECT SUM(ds.UndelivCmdsInDistDB) AS [Undelivered]
    , SUM(ds.DelivCmdsInDistDB) AS [Delivered]
    , ss.name AS [Subscriber]
    , sp.name AS [Publisher]
    , da.publication AS [Publication]
    , da.publisher_db AS [Database]
    , da.name AS [Agent]
FROM dbo.MSdistribution_agents da WITH (READUNCOMMITTED)
    INNER JOIN sys.servers sp WITH (READUNCOMMITTED)
        ON sp.server_id = da.publisher_id
    INNER JOIN sys.servers ss WITH (READUNCOMMITTED)
        ON ss.server_id = da.subscriber_id
    INNER JOIN dbo.MSdistribution_status ds WITH (READUNCOMMITTED)
        ON ds.agent_id = da.id
GROUP BY
    sp.name
    , ss.name
    , da.name
    , da.id
    , da.publication
    , da.publisher_db
ORDER BY undelivered DESC
	, delivered DESC";
                dt = d.ExecuteWithResults(sql).Tables[0];
            }
            return dt;
        }

        /// <summary>
        /// Get average latency/rate history from distribution database, group by 10 minutes
        /// </summary>
        /// <param name="d">Your smo database (distribution)</param>
        /// <param name="startDate">Start DateTime</param>
        /// <param name="endDate">End DateTime</param>
        /// <returns>a datatable</returns>
        public static DataTable GetReplicationLatencyStats(this smo.Database d, DateTime startDate, DateTime endDate)
        {
            DataTable dt = new DataTable();
            if (d.IsDistributor())
            {
                string sql = string.Format(@"IF EXISTS(SELECT * FROM sys.objects WHERE name = 'MSdistribution_history')
BEGIN
    SELECT CONVERT(CHAR(15), time, 121) + '0:00' dt
        , ROUND(AVG(CAST(current_delivery_latency AS bigint)), 0) AS [Average Delivery Latency]
        , ROUND(AVG(CAST(current_delivery_rate AS bigint)), 0) [Average Delivery Rate]
        , db_name() as [Database]
    FROM dbo.MSdistribution_history WITH (READUNCOMMITTED)
    WHERE current_delivery_latency > 0
        AND time BETWEEN '{0}' AND '{1}'
    GROUP BY CONVERT(CHAR(15), time, 121) + '0:00'
    ORDER BY dt DESC
END", startDate.ToString("yyyyMMdd hh:mm:ss"), endDate.ToString("yyyyMMdd hh:mm:ss"));
                dt = d.ExecuteWithResults(sql).Tables[0];
            }
            return dt;
        }

        /// <summary>
        /// Get transactions opened for each replicated databases
        /// Must be executed on distribution db
        /// </summary>
        /// <param name="d">Your smo database (distribution)</param>
        /// <returns>a datatable</returns>
        public static DataTable GetReplicationTransactionsByDatabase(this smo.Database d)
        {
            DataTable dt = new DataTable();
            if (d.IsDistributor())
            {
                string sql = @"SELECT d.publisher_db AS [Database]
	, COUNT(*) AS [Transactions Count]
FROM MSrepl_transactions t
	INNER JOIN MSpublisher_databases d ON t.publisher_database_id = d.id
GROUP BY d.publisher_db
ORDER BY [Transactions Count] DESC";
                dt = d.ExecuteWithResults(sql).Tables[0];
            }
            return dt;
        }

        /// <summary>
        /// Get commands count by article
        /// Must be executed on distribution db
        /// </summary>
        /// <param name="d">Your smo database (distribution)</param>
        /// <returns>a datatable</returns>
        public static DataTable GetReplicationCommandsByArticle(this smo.Database d, string subscriber, string database)
        {
            DataTable dt = new DataTable();
            if (d.IsDistributor())
            {
                string sql = string.Format(@"SELECT s.name AS [Subscriber]
	, sub.publisher_db AS [Database]
	, a.article AS [Article]
	, COUNT(*) AS [Commands Count]
FROM dbo.MSrepl_commands c (NOLOCK)
	INNER JOIN dbo.MSsubscriptions sub (NOLOCK) ON c.publisher_database_id = sub.publisher_database_id
        AND c.article_id = sub.article_id
	INNER JOIN dbo.MSarticles a (NOLOCK) ON sub.publisher_id = a.publisher_id
        AND sub.publication_id = a.publication_id
        AND sub.article_id = a.article_id
	INNER JOIN sys.servers s ON sub.subscriber_id = s.server_id
WHERE s.name = '{0}'
    AND sub.publisher_db = '{1}'
GROUP BY s.name
	, sub.publisher_db
	, a.article
ORDER BY [Commands Count] DESC
    , [Subscriber]", subscriber, database);
                dt = d.ExecuteWithResults(sql).Tables[0];
            }
            return dt;
        }

        /// <summary>
        /// get error from distribution
        /// </summary>
        /// <param name="d">your smo distribution database</param>
        /// <param name="startDate"></param>
        /// <returns>a datatable</returns>
        public static DataTable GetReplicationErrorLog(this smo.Database d, DateTime startDate)
        {
            DataTable dt = new DataTable();
            if (d.IsDistributor())
            {
                string sql = string.Format(@"IF EXISTS(SELECT * FROM sys.objects WHERE name = 'MSrepl_errors')
BEGIN
    SELECT TOP 10000 DB_NAME() [Database]
	    ,  [time]
	    , source_name AS [Source Name]
	    , error_text AS [Error Text]
    FROM dbo.MSrepl_errors WITH (READUNCOMMITTED)
    WHERE [time] >= '{0}'
    order by time desc
END", startDate.ToString("yyyyMMdd hh:mm:ss"));
                dt = d.ExecuteWithResults(sql).Tables[0];
            }
            return dt;
        }
        #endregion

        #region Backups
        /// <summary>
        /// Get the backup history for a given database
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <param name="limit">This param will be used in the SELECT TOP XXX</param>
        /// <returns>a DataTable with the result of the query</returns>
        public static DataTable GetBackupHistory(this smo.Database d, int limit = 1000)
        {
            string sql = string.Format(@"SELECT TOP {0} 
    CASE s.[type]
		WHEN 'D' THEN 
			CASE WHEN s.is_snapshot = 1 THEN 'Snapshot' ELSE 'Full' END
        WHEN 'I' THEN 'Differential'
        WHEN 'L' THEN 'Transaction Log'
        ELSE 'Other' 
	END AS [Backup Type]
    , m.physical_device_name AS [Physical Device Name]
    , CAST(CAST(s.compressed_backup_size / 1048576 AS INT) AS VARCHAR(20)) + ' ' + 'MB' AS [Backup Size (MB)]
    , CAST(CAST(100 - (compressed_backup_size / backup_size * 100) as DECIMAL(9,2)) as VARCHAR(10)) + ' %' AS [Compression Ratio]
    , CAST(DATEDIFF(SECOND, s.backup_start_date, s.backup_finish_date) AS VARCHAR(5)) + ' ' + 'Seconds' AS [Time Taken]
    , s.backup_start_date AS [Backup Start Date]
    , s.backup_finish_date AS [Backup Finish Date]
	, CAST(s.first_lsn AS VARCHAR(25)) AS [First LSN]
	, CAST(s.last_lsn AS VARCHAR(25)) AS [Last LSN]
    , CAST(s.compressed_backup_size / 1048576 as INT) as __bkSizeInt
    , DATEDIFF(second, s.backup_start_date, s.backup_finish_date) __TimeTakenInt
    , CASE s.[type]
		WHEN 'D' THEN 
			CASE WHEN s.is_snapshot = 1 THEN '#327AC1FF' ELSE '#32FF0000' END
        WHEN 'I' THEN '#320AFF0E'
        WHEN 'L' THEN '#32FFFF00'
        ELSE '#00000000'
    END AS __rowColor
FROM msdb.dbo.backupset s (NOLOCK)
    INNER JOIN msdb.dbo.backupmediafamily m (NOLOCK) ON s.media_set_id = m.media_set_id
WHERE s.database_name = '{1}'
ORDER BY backup_start_date DESC
    , backup_finish_date", limit, d.Name);
            if (d.Parent.VersionMajor < 10)
            {
                sql = string.Format(@"SELECT TOP {0}
    CASE s.[type]
        WHEN 'D' THEN 'Full'
        WHEN 'I' THEN 'Differential'
        WHEN 'L' THEN 'Transaction Log'
        ELSE 'Other'
    END AS [Backup Type]
    , m.physical_device_name AS [Physical Device Name]
    , CAST(CAST(s.backup_size / 1048576 AS INT) AS VARCHAR(20)) + ' ' + 'MB' AS [Backup Size (MB)]
    , '-' AS [Compression Ratio]
    , CAST(DATEDIFF(SECOND, s.backup_start_date, s.backup_finish_date) AS VARCHAR(5)) + ' ' + 'Seconds' AS [Time Taken],
    , s.backup_start_date AS [Backup Start Date]
    , s.backup_finish_date AS [Backup Finish Date]
	, s.first_lsn AS [First LSN]
	, s.last_lsn AS [Last LSN]
    , CAST(s.backup_size / 1048576 as INT) as __bkSizeInt
    , DATEDIFF(second, s.backup_start_date, s.backup_finish_date) __TimeTakenInt
    , CASE s.[type]
        WHEN 'D' THEN '#32FF0000'
        WHEN 'I' THEN '#320AFF0E'
        WHEN 'L' THEN '#32FFFF00'
        ELSE '#00000000'
    END AS __rowColor
FROM msdb.dbo.backupset s (NOLOCK)
    INNER JOIN msdb.dbo.backupmediafamily m (NOLOCK) ON s.media_set_id = m.media_set_id
WHERE s.database_name = {1}
ORDER BY backup_start_date DESC
	, backup_finish_date", limit, d.Name);
            }
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion

        #region Audits
        /// <summary>
        /// Get the list of Foreign Keys not indexed. It's often a good idea to index them (not always!)
        /// You can easily customize this query and add/remove columns since Kankuru Datagrid autogenerate columns
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a DataTable with the result of the query.</returns>
        public static DataTable GetFKWithoutIndex(this smo.Database d)
        {
            string sql = @"SELECT s.name AS [Schema Name]
	, OBJECT_NAME(fk.parent_object_id) AS [Table Name]
	, c.name AS [Column Name]
	, fk.name AS [Constraint Name]
	, fk.is_disabled AS [Is Disabled]
FROM sys.foreign_keys fk (NOLOCK)
	INNER JOIN sys.foreign_key_columns fc (NOLOCK) ON fk.OBJECT_ID = fc.constraint_object_id
	INNER JOIN sys.columns c (NOLOCK) ON fc.parent_column_id = c.column_id 
		AND fc.parent_object_id = c.object_id
	INNER JOIN sys.tables t (NOLOCK) ON fk.parent_object_id = t.object_id
	INNER JOIN sys.schemas s (NOLOCK) ON t.schema_id = s.schema_id
WHERE NOT EXISTS (
	SELECT * FROM sys.tables t  (NOLOCK)
		INNER JOIN sys.indexes i (NOLOCK) ON i.object_id = t.object_id  
		INNER JOIN sys.columns c2 (NOLOCK) ON t.object_id = c2.object_id  
		INNER JOIN sys.index_columns ic (NOLOCK) ON ic.object_id = t.object_id 
			AND i.index_id = ic.index_id 
			AND ic.column_id = c2.column_id  
	WHERE t.type = 'U' 
		AND t.name = OBJECT_NAME(fk.parent_object_id)
		AND c2.name = c.name)";
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get duplicated indexes. Duplicated indexes are 2 indexes using the same column.
        /// Inspired by http://blog.developpez.com/sqlpro/p9263/langage-sql-norme/une_requete_recherchant_les_index_anorma
        /// You can easily customize this query and add/remove columns since Kankuru Datagrid autogenerate columns
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a DataTable with the result of the query.</returns>
        public static DataTable GetDuplicatedIndex(this smo.Database d)
        {
            string sqlFilter = @", i1.filter_definition AS [Filter Definition]
, i2.filter_definition AS [Similar Filter Definition] ";

            if (d.Parent.Version.Major < 10)
                sqlFilter = string.Empty;

            string sql = string.Format(@"WITH T0 AS (
    SELECT ic.object_id
        , index_id
        , c.column_id
        , key_ordinal
        ,  CASE is_descending_key
                WHEN '0' THEN 'ASC'
                WHEN '1' THEN 'DESC'
            END AS sens
        , c.name AS column_name
        , ROW_NUMBER() OVER(PARTITION BY ic.object_id, index_id ORDER BY key_ordinal DESC) AS N
        , is_included_column
    FROM sys.index_columns AS ic
        INNER JOIN sys.columns AS c ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
    WHERE key_ordinal > 0
        AND  index_id > 0)
, T1 AS (
    SELECT object_id
        , index_id
        , column_id
        , key_ordinal
        , N
        , CASE WHEN is_included_column = 0 THEN CAST(column_name AS VARCHAR(MAX)) + ' ' + sens ELSE '' END AS LitteralComp
        , CASE WHEN is_included_column = 0 THEN CAST(column_id AS VARCHAR(MAX)) + SUBSTRING(sens, 1, 1) ELSE '' END AS MathComp
        , MAX(N) OVER(PARTITION BY object_id, index_id) AS CMax
        , CASE WHEN is_included_column = 1 THEN CAST(column_name AS VARCHAR(MAX)) ELSE '' END AS IncludedColumns
    FROM T0
    WHERE key_ordinal = 1
    UNION  ALL
    SELECT T0.object_id
        , T0.index_id
        , T0.column_id
        , T0.key_ordinal
        , T0.N
        , LitteralComp + CASE WHEN is_included_column = 0 THEN  ', ' + CAST(T0.column_name AS VARCHAR(MAX)) + ' ' + T0.sens ELSE '' END
        , MathComp + CASE WHEN is_included_column = 0 THEN CAST(T0.column_id AS VARCHAR(MAX)) + SUBSTRING(T0.sens, 1, 1) ELSE '' END
        , T1.CMax, IncludedColumns + CASE WHEN is_included_column = 1 THEN ', ' + CAST(column_name AS VARCHAR(MAX)) ELSE '' END
    FROM T0
        INNER JOIN T1 ON T0.object_id = T1.object_id
            AND T0.index_id = T1.index_id
            AND T0.key_ordinal = T1.key_ordinal + 1)
, T2 AS (
    SELECT object_id
        , index_id
        , LitteralComp
        , MathComp
        , CMax
        , IncludedColumns
    FROM T1
    WHERE N = 1)
, T4 AS (
    SELECT T2.object_id
        , T2.index_id
        , T3.index_id AS index_id_anomalie
        , T2.LitteralComp AS IndexKey
        , T3.LitteralComp AS SimilarIndexKey
        , T2.IncludedColumns
        , T3.IncludedColumns AS SimilarIncludedColumns
        , CASE WHEN T2.MathComp = T3.MathComp THEN 'Duplicated' WHEN T2.MathComp LIKE T3.MathComp +'%' THEN 'Included' END AS DuplicationType
        , ABS(T2.CMax - T3.CMax) AS Distance
    FROM T2
        INNER JOIN T2 AS T3 ON T2.object_id = T3.object_id
            AND T2.index_id <> T3.index_id
            AND T2.MathComp LIKE T3.MathComp +'%')
SELECT s.name +'.' + o.name AS [Table Name]
    , i1.name AS [Index Name]
    , i2.name AS [Similar Index]
    , T4.DuplicationType AS [Duplication Type]
    , T4.Distance
    , T4.IndexKey AS [Index Key]
    , T4.SimilarIndexKey AS [Similar Index Key]
    , T4.IncludedColumns AS [Included Columns]
    , T4.SimilarIncludedColumns AS [Similar IncludedColumns]
{0}
	, i1.type_desc AS [Index Type]
	, i2.type_desc AS [Similar Index Type]
    , i1.is_primary_key AS [Is Primary Key]
    , i2.is_primary_key AS [Is Primary Key Similar]
    , i1.is_unique AS [Is Unique Index]
    , i2.is_unique AS [Is Unique Similar Index]
    , i1.is_unique_constraint AS [Is Unique Constraint]
    , i2.is_unique_constraint AS [Is Unique Constraint Similar]
FROM T4
    INNER JOIN sys.objects AS o ON T4.object_id = o.object_id
    INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    INNER JOIN sys.indexes AS i1 ON T4.object_id = i1.object_id
        AND T4.index_id = i1.index_id
    INNER JOIN sys.indexes AS i2 ON T4.object_id = i2.object_id
        AND T4.index_id_anomalie = i2.index_id
WHERE o.type IN ('U', 'V')
ORDER  BY [Table Name]
    , [Index Name]", sqlFilter);
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get list of Heap Indexes for the specified database
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a DataTable with the result of the query.</returns>
        public static DataTable GetHeapIndex(this smo.Database d)
        {
            return d.ExecuteWithResults(@"SELECT s.name + '.' + t.name AS [Table Name]
	, create_date AS [Create Date]
	, modify_date AS [Modify Date]
FROM sys.indexes i (NOLOCK)
	INNER JOIN sys.tables t (NOLOCK) ON t.object_id = i.object_id
	INNER JOIN sys.schemas s (NOLOCK) ON t.schema_id = s.schema_id
WHERE i.[type] = 0 
ORDER BY [Table Name]").Tables[0];
        }

        /// <summary>
        /// Get list of Tables without Primary Key
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a DataTable with the result of the query.</returns>
        public static DataTable GetTableWithoutPK(this smo.Database d)
        {
            return d.ExecuteWithResults(@"SELECT s.name as [Schema Name]
	, t.name as [Table Name]
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
	AND NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
		WHERE c.CONSTRAINT_TYPE = 'PRIMARY KEY'
			AND s.name = c.TABLE_SCHEMA
			AND t.name = c.TABLE_NAME
	)
ORDER BY s.name
	, t.name").Tables[0];
        }

        /// <summary>
        /// Get list of Tables without Clustered Index
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a DataTable with the result of the query.</returns>
        public static DataTable GetTableWithoutClusteredIndex(this smo.Database d)
        {
            return d.ExecuteWithResults(@"SELECT s.name AS [Schema Name]
	, t.name AS [Table Name]
FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
	AND NOT EXISTS (
		SELECT *
		FROM sys.tables st
			INNER JOIN sys.schemas ss ON st.schema_id = ss.schema_id
			INNER JOIN sys.indexes i ON i.object_id = st.object_id
		WHERE i.type = 1
			AND s.name = ss.name
			AND t.name = st.name
	)
ORDER BY t.name").Tables[0];
        }

        /// <summary>
        /// Get list of Tables with access statistics.
        /// Note : these statistics are deleted when SQL Server restarts
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a DataTable with the result of the query.</returns>
        public static DataTable GetLastAccessByTable(this smo.Database d)
        {
            return d.ExecuteWithResults(@"WITH agg as (
	SELECT [object_id] 
		, last_user_seek 
		, last_user_scan 
		, last_user_lookup 
		, last_user_update 
	FROM sys.dm_db_index_usage_stats (NOLOCK)
    WHERE database_id = DB_ID()
) 
SELECT MAX(last_read) AS last_read
	, max (last_write) as last_write
	, object_id
INTO #TEMP
FROM (
	SELECT [object_id] 
		, last_user_seek 
		, null 
	FROM agg 
	UNION all 
	SELECT [object_id] 
		, last_user_scan 
		, null 
	FROM agg 
	UNION all 
	SELECT [object_id] 
		, last_user_lookup 
		, null 
	FROM agg 
	UNION all 
	SELECT [object_id] 
		, null 
		, last_user_update 
	FROM agg) AS x 
	(object_id
	, last_read 
	, last_write) 
GROUP BY object_id

SELECT s.name AS [Schema Name]
	, t.name AS [Table Name]
	, temp.last_read AS [Last Read]
	, temp.last_write AS [Last Write]
	, CASE WHEN COALESCE(temp.last_read, 0) > COALESCE(temp.last_write, 0) THEN temp.last_read ELSE temp.last_write END AS [Last Access]
    , t.create_date AS [Create Date]
    , t.modify_date AS [Last Modify Date]
FROM sys.tables t (NOLOCK)
INNER JOIN sys.schemas s (NOLOCK) ON t.schema_id = s.schema_id
LEFT JOIN #TEMP temp (NOLOCK) ON t.object_id = temp.object_id

ORDER BY [Last Access] DESC

DROP TABLE #TEMP").Tables[0];
        }

        public static DataTable GetLastExecutionByProcedure(this smo.Database d)
        {
            return d.ExecuteWithResults(@"SELECT sc.name AS [Schema]
	, p.name AS [Procedure Name]
	, SUM(execution_count) AS [Execution Count]
	, MAX (last_execution_time) AS [Last Execution Time]
FROM sys.procedures p
	INNER JOIN sys.schemas sc ON p.schema_id = sc.schema_id
	LEFT JOIN sys.dm_exec_procedure_stats s ON sc.name = OBJECT_SCHEMA_NAME(s.object_id, database_id)
		AND p.name = OBJECT_NAME(s.object_id, database_id)
		AND s.database_id = DB_ID()
GROUP BY sc.name
	, p.name
ORDER BY sc.name
	, p.name").Tables[0];
        }

        /// <summary>
        /// Find all columns in a database with the same name but with different datatype
        /// </summary>
        /// <param name="d">your smo Database</param>
        /// <returns>a datatable</returns>
        public static DataTable GetColumnWithSameNameButDifferentType(this smo.Database d)
        {
            string _sql = @"SELECT DISTINCT columnName AS [Column Name]
FROM
(
	SELECT ss.columnName
		, ss.typeName
		, ss.max_length
		, ss.precision
		, SUM(ss.nb) AS sumnb
		, ROW_NUMBER() OVER (PARTITION BY ss.columnName ORDER BY ss.columnName) AS rid
	FROM
	(
		SELECT c.name AS columnName
			, ty.name AS typeName
			, c.max_length
			, c.precision
			, COUNT(*) AS nb
		FROM sys.columns c (NOLOCK)
			INNER JOIN sys.tables t (NOLOCK) on c.object_id = t.object_id
			INNER JOIN sys.types ty (NOLOCK) on c.user_type_id = ty.user_type_id
		GROUP BY c.name
			, ty.name
			, c.max_length
			, c.precision
	)ss
	GROUP BY ss.columnName
		, ss.typeName
		, ss.max_length
		, ss.precision
)sss
WHERE rid > 1
ORDER BY sss.columnName";
            return d.ExecuteWithResults(_sql).Tables[0];
        }

        /// <summary>
        /// Get all column occurences in a database for a given column name.
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <param name="column">Datatable</param>
        /// <returns></returns>
        public static DataTable GetAllColumnsWithName(this smo.Database d, string column)
        {
            string _sql = string.Format(@"SELECT s.name + '.' + t.name AS tablename
	, ty.name AS typeName
	, c.max_length
	, c.precision
FROM sys.columns c (NOLOCK)
	INNER JOIN sys.tables t (NOLOCK) ON c.object_id = t.object_id
    INNER JOIN sys.schemas s (NOLOCK) ON t.schema_id = s.schema_id
	INNER JOIN sys.types ty (NOLOCK) ON c.user_type_id = ty.user_type_id
WHERE c.name = '{0}'
ORDER BY typeName
	, c.max_length
	, c.precision
	, t.name", column);
            return d.ExecuteWithResults(_sql).Tables[0];
        }

        /// <summary>
        /// Get Disabled index list in the database
        /// </summary>
        public static DataTable GetDisabledIndex(this smo.Database d)
        {
            string sqlFilter = @", i.filter_definition ";

            if (d.Parent.Version.Major < 10)
                sqlFilter = string.Empty;
            string sql = string.Format(@"WITH cteCol(index_id, object_id, schema_name, table_name, index_name, index_column_id, column_name)
AS(
	SELECT i.index_id
		, i.object_id
		, s.name AS schema_name
		, t.name AS table_name
		, i.name AS index_name
		, index_column_id
		, CAST(c.name AS VARCHAR(MAX)) AS column_name
	FROM sys.tables t
		INNER JOIN sys.schemas s on t.schema_id = s.schema_id
		INNER JOIN sys.indexes i on i.object_id = t.object_id
		INNER JOIN sys.index_columns ic on ic.object_id = t.object_id
			AND ic.index_id=i.index_id
		INNER JOIN sys.columns c on c.object_id = t.object_id
			AND ic.column_id = c.column_id
    WHERE ic.index_column_id = 1
		AND i.is_disabled = 1
UNION ALL
	SELECT i.index_id
		, i.object_id
		, s.name
		, t.name
		, i.name
		, ic.index_column_id
		, CAST(cteCol.column_name + ',' + c.name AS VARCHAR(MAX))
	FROM sys.tables t
		INNER JOIN sys.schemas s on t.schema_id = s.schema_id
		INNER JOIN sys.indexes i on i.object_id = t.object_id
		INNER JOIN sys.index_columns ic on ic.object_id = t.object_id
			AND ic.index_id = i.index_id
		INNER JOIN sys.columns c on c.object_id = t.object_id
			AND ic.column_id = c.column_id
		INNER JOIN cteCol on cteCol.index_column_id + 1 = ic.index_column_id
			AND cteCol.schema_name = s.name
			AND cteCol.table_name = t.name
			AND cteCol.index_name = i.name
	WHERE i.is_disabled = 1)
SELECT cteCol.schema_name + '.' + cteCol.table_name AS [Table Name]
	, cteCol.index_name AS [Index Name]
	, cteCol.column_name AS [Column Name]
	, i.type_desc
	, i.is_unique
{0}
FROM cteCol
	INNER JOIN (
		SELECT schema_name
			, table_name
			, index_name
			, MAX(index_column_id) index_column_id
		FROM cteCol
		GROUP BY schema_name
		, table_name
		, index_name) mx ON cteCol.schema_name = mx.schema_name
			AND cteCol.table_name = mx.table_name
			AND cteCol.index_name = mx.index_name
			AND cteCol.index_column_id = mx.index_column_id
	INNER JOIN sys.indexes i ON cteCol.index_id = i.index_id
		AND cteCol.object_id = i.object_id
ORDER BY [Table Name]
	, [Index Name]
	, [Column Name]", sqlFilter);
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #endregion

        #region Memory
        /// <summary>
        /// Get Buffer detail for a database. What is the memory usage by object ?
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>DataTable with the result of the query</returns>
        public static DataTable GetBufferStats(this smo.Database d)
        {
            string compression1 = "		, p.data_compression_desc AS CompressionType ";
            string compression2 = "		, p.data_compression_desc ";
            string compression3 = "	, cte.CompressionType AS [Compression Type]";
            if (d.Parent.VersionMajor < 10)
            {
                compression1 = string.Empty;
                compression2 = string.Empty;
                compression3 = string.Empty;
            }
            string sql = string.Format(@"WITH cte AS
(
    SELECT p.object_id
        , p.index_id
        , COUNT(*) / 128 AS Buffer_size
        , COUNT(*) AS BufferCount
        , a.type_desc
        , p.rows
        {0}
    FROM sys.dm_os_buffer_descriptors b  (NOLOCK)
        LEFT JOIN sys.allocation_units a (NOLOCK) ON a.allocation_unit_id = b.allocation_unit_id
        LEFT JOIN sys.partitions p (NOLOCK) ON a.container_id = p.partition_id
    WHERE b.database_id = CONVERT(int, DB_ID())
    GROUP BY p.object_id
        , p.index_id
        {1}
        , a.type_desc
        , p.rows
)
SELECT COALESCE(COALESCE(OBJECT_SCHEMA_NAME(cte.object_id), '') + '.' + OBJECT_NAME(cte.object_id), 'Unused') AS [Table]
    , i.name AS [Index]
    , cte.Buffer_size AS [Buffer Size]
    , cte.BufferCount AS [Buffer Count]
    {2}
    , cte.type_desc AS [Allocation Unit Type]
    , cte.rows AS [Rows]
FROM cte
    LEFT JOIN sys.indexes i(NOLOCK) ON cte.index_id = i.index_id
        AND cte.object_id = i.object_id
ORDER BY cte.BufferCount DESC", compression1, compression2, compression3);
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion

        #region Indexes
        /// <summary>
        /// Get expensive indexes : when index writes are greater than reads
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a datatable</returns>
        public static DataTable GetExpensiveIndexes(this smo.Database d)
        {
            string sql = @"SELECT sc.name + '.' + o.name AS [Table]
	, i.name AS [Index]
	, s.user_updates AS [User Update]
	, s.user_seeks AS [User Seeks]
	, s.user_scans AS [User Scans]
	, s.user_lookups AS [User Lookups]
	, s.user_seeks + s.user_scans + s.user_lookups AS [Total Read]
FROM sys.indexes i (nolock)
	INNER JOIN sys.dm_db_index_usage_stats s (nolock) ON s.[object_id] = i.[object_id]
		AND s.index_id = i.index_id
	INNER JOIN sys.objects o ON i.object_id = o.object_id
	INNER JOIN sys.schemas sc ON o.schema_id = sc.schema_id
WHERE OBJECTPROPERTY(i.[object_id], 'ismsshipped') = 0
	AND i.name IS NOT NULL
	AND s.user_updates > (user_seeks + user_scans + user_lookups)
ORDER BY user_updates DESC";
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get missing indexes : based on missing index dmv
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a datatable</returns>
        public static DataTable GetMissingIndexes(this smo.Database d)
        {
            string sql = @"SELECT mid.[statement] AS [Table]
	, mid.equality_columns AS [Equality Columns]
	, mid.inequality_columns AS [Inequality Columns]
	, mid.included_columns AS [Included Columns]
	, migs.unique_compiles AS [Unique Compiles]
	, migs.last_user_seek AS [Last User Seek]
	, migs.user_seeks AS [User Seeks]
	, ROUND(migs.avg_total_user_cost, 2) AS [Avg Total User Cost]
	, migs.avg_user_impact AS [Avg User Impact]
	, ROUND(user_seeks * avg_total_user_cost * (avg_user_impact * 0.01), 2) AS [Index Advantage]
	, 'CREATE NONCLUSTERED INDEX [IX_' + OBJECT_NAME(mid.OBJECT_ID,mid.database_id) + '_'
		+ REPLACE(REPLACE(REPLACE(ISNULL(mid.equality_columns,''),', ','_'),'[',''),']','') +
		CASE
		WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN '_'
		ELSE ''
		END
		+ REPLACE(REPLACE(REPLACE(ISNULL(mid.inequality_columns,''),', ','_'),'[',''),']','')
		+ ']'
		+ ' ON ' + mid.statement
		+ ' (' + ISNULL (mid.equality_columns,'')
		+ CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN ', ' ELSE
		'' END
		+ ISNULL (mid.inequality_columns, '')
		+ ')'
		+ ISNULL (' INCLUDE (' + mid.included_columns + ')', '') AS [__script]
FROM sys.dm_db_missing_index_group_stats AS migs WITH (NOLOCK)
    INNER JOIN sys.dm_db_missing_index_groups AS mig WITH (NOLOCK) ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details AS mid WITH (NOLOCK) ON mig.index_handle = mid.index_handle
WHERE mid.database_id = DB_ID()
ORDER BY [Index Advantage] DESC";
            return d.ExecuteWithResults(sql).Tables[0];
        }
        #endregion


        public static DataTable GetTableTreeMaps(this smo.Database d, int minSize, int maxSize, bool withTables, bool withViews)
        {
            string objectTypeFilter = string.Empty;
            if (withTables == false || withViews == false)
            {
                if (!withTables)
                {
                    objectTypeFilter = " and type != 'U' ";
                }
                if (!withViews)
                {
                    objectTypeFilter = " and type != 'V' ";
                }
            }

            string sql = string.Format(@"WITH cte AS(
	SELECT
		a3.name AS [schemaname],
		a2.name AS [tablename],
        a2.type,
        a2.type_desc,
		a1.rows AS row_count,
		(a1.reserved + ISNULL(a4.reserved,0))* 8.0 / 1024 AS reserved,
		a1.data * 8.0 / 1024 AS data,
		(CASE WHEN (a1.used + ISNULL(a4.used,0)) > a1.data THEN (a1.used + ISNULL(a4.used,0)) - a1.data ELSE 0 END) * 8.0 / 1024 AS index_size,
		(CASE WHEN (a1.reserved + ISNULL(a4.reserved,0)) > a1.used THEN (a1.reserved + ISNULL(a4.reserved,0)) - a1.used ELSE 0 END) * 8.0 / 1024 AS unused
	FROM
		(SELECT ps.object_id
			, SUM(
				CASE
					WHEN (ps.index_id < 2) THEN row_count
					ELSE 0
				END) AS [rows]
			, SUM(ps.reserved_page_count) AS reserved
            , SUM(
				CASE
					WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count)
					ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count)
				END) AS data
            , SUM (ps.used_page_count) AS used
		FROM sys.dm_db_partition_stats ps
		GROUP BY ps.object_id) AS a1
	LEFT OUTER JOIN
		(SELECT it.parent_id
			, SUM(ps.reserved_page_count) AS reserved
			, SUM(ps.used_page_count) AS used
		 FROM sys.dm_db_partition_stats ps
		    INNER JOIN sys.internal_tables it ON (it.object_id = ps.object_id)
		 WHERE it.internal_type IN (202,204)
		 GROUP BY it.parent_id) AS a4 ON (a4.parent_id = a1.object_id)
	INNER JOIN sys.all_objects a2  ON ( a1.object_id = a2.object_id )
	INNER JOIN sys.schemas a3 ON (a2.schema_id = a3.schema_id)
	WHERE a2.type <> N'S' and a2.type <> N'IT'
)
select schemaname
	, tablename
	, row_count
    , type
    , type_desc
	, reserved
	, data
	, index_size
	, unused
	, CASE WHEN reserved != 0 THEN index_size * 100.0 / reserved ELSE 0 END AS IndexPercent
FROM cte
WHERE reserved >= {0}
    AND reserved < {1}
    {2}
ORDER BY reserved DESC", minSize, maxSize, objectTypeFilter);
            return d.ExecuteWithResults(sql).Tables[0];
        }


        public static DataTable DashboardSsrs(this smo.Database d, TimeSpan logFrom)
        {
            string sql = string.Format(@"SELECT EL.TimeStart AS LogDate
, EL.Status + '"" on ""' +     CASE(ReportAction)
        WHEN 1 THEN 'Render'
        WHEN 2 THEN 'BookmarkNavigation'
        WHEN 3 THEN 'DocumentMapNavigation'
        WHEN 4 THEN 'DrillThrough'
        WHEN 5 THEN 'FindString'
        WHEN 6 THEN 'GetDocumentMap'
        WHEN 7 THEN 'Toggle'
        WHEN 8 THEN 'Sort'
        WHEN 9 THEN 'Execute'
        WHEN 10 THEN 'RenderEdit'
        ELSE 'Unknown'
        END + '"" the report ""' + ISNULL(C.Name, EL.ReportID) + '"". The username is ""' + EL.UserName + '""' AS texte
FROM ExecutionLogStorage EL (NOLOCK)
	LEFT JOIN Catalog C (NOLOCK) ON EL.ReportID = C.ItemID
WHERE EL.Status != 'rsSuccess'
	AND EL.TimeStart >= DATEADD(DAY, -{0}, DATEADD(HOUR, -{1}, DATEADD(MINUTE, -{2}, GETDATE())))
UNION ALL
SELECT LastRunTime AS LogDate
   , 'Subscription ' + coalesce(c.Name, '') + ' : ' + LastStatus AS texte
FROM Subscriptions s
    LEFT JOIN Catalog c ON s.Report_OID = c.ItemID
WHERE s.LastStatus LIKE 'Failure%'
    AND LastRunTime >= DATEADD(DAY, -{0}, DATEADD(HOUR, -{1}, DATEADD(MINUTE, -{2}, GETDATE())))
ORDER BY LogDate DESC", logFrom.Days, logFrom.Hours, logFrom.Minutes);
            if (d.Parent.VersionMajor < 10)
            {
                sql = string.Format(@"SELECT EL.TimeStart AS LogDate
    , EL.Status + '"" on the report ""' + ISNULL(C.Name, EL.ReportID) + '"". The username is ""' + EL.UserName + '""' AS texte
FROM ExecutionLog EL (NOLOCK)
	LEFT JOIN Catalog C (NOLOCK) ON EL.ReportID = C.ItemID
WHERE EL.Status != 'rsSuccess'
	AND EL.TimeStart >= DATEADD(DAY, -{0}, DATEADD(HOUR, -{1}, DATEADD(MINUTE, -{2}, GETDATE())))
UNION ALL
SELECT LastRunTime AS LogDate
   , 'Subscription ' + coalesce(c.Name, '') + ' : ' + LastStatus AS texte
FROM Subscriptions s
    LEFT JOIN Catalog c ON s.Report_OID = c.ItemID
WHERE s.LastStatus like 'Failure%'
    AND LastRunTime >= DATEADD(DAY, -{0}, DATEADD(HOUR, -{1}, DATEADD(MINUTE, -{2}, GETDATE())))
ORDER BY LogDate DESC", logFrom.Days, logFrom.Hours, logFrom.Minutes);
            }
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #region Statistics
        /// <summary>
        /// Get the statistics options for the database
        /// </summary>
        public static DataTable StatisticsConfiguration(this smo.Database d)
        {
            string sql = @"DECLARE @SQL NVARCHAR(4000)
SET @SQL = 'SELECT {0} AS is_auto_update_stats_on
    , {1} AS is_auto_update_stats_async_on
    , {2} AS is_auto_create_stats_on
    , {3} AS is_auto_create_stats_incremental_on
FROM sys.databases
WHERE database_id = DB_ID()'

IF EXISTS(SELECT *
FROM sys.all_columns c
WHERE c.name = 'is_auto_update_stats_on'

    AND OBJECT_NAME(c.object_id) = 'databases')
BEGIN
    SET @SQL = REPLACE(@SQL, '{0}', 'is_auto_update_stats_on')
END
ELSE
BEGIN
    SET @SQL = REPLACE(@SQL, '{0}', '''N/A''')
END

IF EXISTS(SELECT *
FROM sys.all_columns c
WHERE c.name = 'is_auto_update_stats_async_on'

    AND OBJECT_NAME(c.object_id) = 'databases')
BEGIN
    SET @SQL = REPLACE(@SQL, '{1}', 'is_auto_update_stats_async_on')
END
ELSE
BEGIN
    SET @SQL = REPLACE(@SQL, '{1}', '''N/A''')
END

IF EXISTS(SELECT *
FROM sys.all_columns c
WHERE c.name = 'is_auto_create_stats_on'

    AND OBJECT_NAME(c.object_id) = 'databases')
BEGIN
    SET @SQL = REPLACE(@SQL, '{2}', 'is_auto_create_stats_on')
END
ELSE
BEGIN
    SET @SQL = REPLACE(@SQL, '{2}', '''N/A''')
END

IF EXISTS(SELECT *
FROM sys.all_columns c
WHERE c.name = 'is_auto_create_stats_incremental_on'

    AND OBJECT_NAME(c.object_id) = 'databases')
BEGIN
    SET @SQL = REPLACE(@SQL, '{3}', 'is_auto_create_stats_incremental_on')
END
ELSE
BEGIN
    SET @SQL = REPLACE(@SQL, '{3}', '''N/A''')
END

EXECUTE sys.sp_executesql @SQL";
            return d.ExecuteWithResults(sql).Tables[0];
        }

        /// <summary>
        /// Get informations for each statistics in the database
        /// Inspired by https://littlekendra.com/2016/12/06/when-did-sql-server-last-update-that-statistic-how-much-has-been-modified-since-and-what-columns-are-in-the-stat/
        /// </summary>
        public static DataTable StatisticsProperties(this smo.Database d, bool? autoCreated, bool? linkedToIndex)
        {
            DataSet dsHasDmDbStatsProperties = d.ExecuteWithResults("SELECT COUNT(*) AS hasDmDbStatsProperties FROM sys.all_objects WHERE name = 'dm_db_stats_properties'");
            bool hasDmDbStatsProperties = false;
            if (dsHasDmDbStatsProperties.Tables[0].Rows.Count > 0 && dsHasDmDbStatsProperties.Tables[0].Rows[0]["hasDmDbStatsProperties"].ToString() != "0")
                hasDmDbStatsProperties = true;

            DataSet dsHasIsIncremental = d.ExecuteWithResults("SELECT COUNT(*) AS hasIsIncremental FROM sys.all_columns c WHERE c.name = 'is_incremental' AND OBJECT_NAME(object_id) = 'stats'");
            bool hasIsIncremental = false;
            if (dsHasIsIncremental.Tables[0].Rows.Count > 0 && dsHasIsIncremental.Tables[0].Rows[0]["hasIsIncremental"].ToString() != "0")
                hasIsIncremental = true;

            DataSet dsHasIsTemporary = d.ExecuteWithResults("SELECT COUNT(*) AS hasIsTemporary FROM sys.all_columns c WHERE c.name = 'is_temporary' AND OBJECT_NAME(object_id) = 'stats'");
            bool hasIsTemporary = false;
            if (dsHasIsTemporary.Tables[0].Rows.Count > 0 && dsHasIsTemporary.Tables[0].Rows[0]["hasIsTemporary"].ToString() != "0")
                hasIsTemporary = true;

            string filterHasIncremental = string.Empty;
            if (hasIsIncremental)
                filterHasIncremental = " , s.is_incremental AS [Is Incremental] ";

            string filterHasTemporary = string.Empty;
            if (hasIsTemporary)
                filterHasTemporary = " , s.is_temporary AS [Is Temporary] ";

            string filterAutoCreated = string.Empty;
            if (autoCreated.HasValue)
            {
                if (autoCreated.Value)
                {
                    filterAutoCreated = " AND s.auto_created = 1 ";
                }
                else
                {
                    filterAutoCreated = " AND s.auto_created = 0 ";
                }
            }
            string filterLinkedToIndex = string.Empty;
            string filterLinkedToIndex2 = string.Empty;
            if (linkedToIndex.HasValue)
            {
                if (linkedToIndex.Value)
                {
                    filterLinkedToIndex = " AND i.index_id IS NOT NULL ";
                    filterLinkedToIndex2 = " AND i.id IS NOT NULL ";
                }
                else
                {
                    filterLinkedToIndex = " AND i.index_id IS NULL ";
                    filterLinkedToIndex2 = " AND i.id IS NULL ";
                }
            }

            string sql = string.Format(@"SELECT sch.name + '.' + o.name AS [Table]
	, s.name AS StatName
	, STUFF((
			SELECT
				', ' + COL_NAME(sic.object_id, sic.column_id) AS [Name]
			FROM sys.stats st
				INNER JOIN sys.stats_columns sic ON sic.stats_id=st.stats_id
					AND sic.object_id=st.object_id
			WHERE st.stats_id = s.stats_id
				AND st.object_id = s.object_id
			FOR XML PATH(''), TYPE
		).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS [Columns]
	, sp.last_updated AS [Last Updated]
	, sp.rows AS [Rows]
	, sp.rows_sampled AS [Rows Sampled]
	, sp.modification_counter AS [Modification Counter]
	, CASE WHEN sp.rows > 0 THEN sp.modification_counter * 100 / sp.rows ELSE 0 END [Modification %]
	, sp.unfiltered_rows AS [Unfiltered Rows]
	, sp.steps AS [Steps]
	, s.auto_created AS [Auto Created]
	, s.user_created AS [User Created]
	, s.has_filter AS [Has Filter]
{0}
{1}
	, s.no_recompute AS [No Recompute]
	, CASE WHEN i.index_id IS NULL THEN 'False' ELSE 'True' END AS [Is Index Linked]
	, ius.user_seeks AS [User Seeks]
	, ius.user_scans AS [User Scans]
	, ius.user_lookups AS [User Lookups]
	, ius.user_updates AS [User Updates]
	, ius.last_user_seek AS [Last User Seek]
	, ius.last_user_scan AS [Last User Scan]
	, ius.last_user_lookup AS [Last User Lookup]
	, ius.last_user_update AS [Last User Update]
FROM sys.stats s
	CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
	INNER JOIN sys.objects o ON s.object_id = o.object_id
	INNER JOIN sys.schemas sch ON o.schema_id = sch.schema_id
	LEFT JOIN sys.indexes i on s.object_id = i.object_id
	    AND s.name = i.name
	LEFT JOIN sys.dm_db_index_usage_stats ius ON i.index_id = ius.index_id
	    AND i.object_id = ius.object_id
	    AND ius.database_id = DB_ID()
WHERE OBJECTPROPERTY(o.object_id, 'IsUserTable') = 1
{2}
{3}
ORDER BY modification_counter DESC
    , [Table]
    , StatName", filterHasIncremental, filterHasTemporary, filterAutoCreated, filterLinkedToIndex);

            if(!hasDmDbStatsProperties)
            {
                sql = string.Format(@"WITH cteIndexStats AS
(
    SELECT id
        , ind.name
        , rowmodctr
    FROM sys.sysindexes AS sysind
        INNER JOIN sys.indexes AS ind ON sysind.id = ind.object_id
            AND sysind.indid=ind.index_id
),
cteColumnStats AS
(
    SELECT id
        , rowmodctr
    FROM sys.sysindexes as sysind
    WHERE sysind.indid in (0,1)
)
SELECT OBJECT_NAME(o.object_id) AS [Table]
    , s.name AS StatName
    , STUFF((
        SELECT
            ', ' + COL_NAME(sic.object_id, sic.column_id) AS [Name]
        FROM sys.stats st
            INNER JOIN sys.stats_columns sic ON sic.stats_id=st.stats_id
                AND sic.object_id=st.object_id
        WHERE st.stats_id = s.stats_id
            AND st.object_id = s.object_id
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS [Columns]
    , STATS_DATE(s.object_id, s.stats_id) AS [Last Updated]
    , ISNULL(i.rowmodctr, c.rowmodctr) AS [Estimated Modification Counter]
    , s.auto_created AS [Auto Created]
    , s.is_temporary AS [Is Temporary]
    , s.no_recompute AS [No Recompute]
FROM sys.stats s
    INNER JOIN sys.objects o ON s.object_id=o.object_id
    INNER JOIN sys.schemas sc ON o.schema_id=sc.schema_id
    LEFT JOIN cteIndexStats i ON i.id = o.object_id
    LEFT JOIN cteColumnStats c ON c.id = o.object_id
WHERE OBJECTPROPERTY(o.object_id, 'IsUserTable') = 1
{0}
{1}
ORDER BY [Estimated Modification Counter] DESC
    , [Table]
    , [StatName]", filterAutoCreated, filterLinkedToIndex2);
            }
            return d.ExecuteWithResults(sql).Tables[0];
        }

        #endregion
    }
}
