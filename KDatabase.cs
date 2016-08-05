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
        #endregion

        #region Backups
        /// <summary>
        /// Get the backup history for a given database
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <param name="limit">This param will be used in the SELECT TOP XXX</param>
        /// <returns>a dataset with the result of the query</returns>
        public static DataSet GetBackupHistory(this smo.Database d, int limit = 1000)
        {
            string sql = string.Format(@"SELECT TOP {0} 
    CASE s.[type]
        WHEN 'D' THEN 'Full'
        WHEN 'I' THEN 'Differential'
        WHEN 'L' THEN 'Transaction Log'
        ELSE 'Other' 
	END AS BackupType
    , m.physical_device_name
    , CAST(CAST(s.compressed_backup_size / 1048576 AS INT) AS VARCHAR(20)) + ' ' + 'MB' AS bkSize
    , CAST(CAST(100 - (compressed_backup_size / backup_size * 100) as DECIMAL(9,2)) as VARCHAR(10)) + ' %' AS ratiocompression
    , CAST(DATEDIFF(SECOND, s.backup_start_date, s.backup_finish_date) AS VARCHAR(5)) + ' ' + 'Seconds' AS TimeTaken
    , s.backup_start_date
    , s.backup_finish_date
	, s.first_lsn AS first_lsn
	, s.last_lsn AS last_lsn
    , CAST(s.compressed_backup_size / 1048576 as INT) as bkSizeInt
    , DATEDIFF(second, s.backup_start_date, s.backup_finish_date) TimeTakenInt
    , CASE s.[type]
        WHEN 'D' THEN '#32FF0000'
        WHEN 'I' THEN '#320AFF0E'
        WHEN 'L' THEN '#32FFFF00'
        ELSE '#00000000'
    END AS rowColor
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
    END AS BackupType
    , m.physical_device_name
    , CAST(CAST(s.backup_size / 1048576 AS INT) AS VARCHAR(20)) + ' ' + 'MB' AS bkSize
    , '-' as ratiocompression
    , CAST(DATEDIFF(SECOND, s.backup_start_date, s.backup_finish_date) AS VARCHAR(5)) + ' ' + 'Seconds' AS TimeTaken,
    , s.backup_start_date
    , s.backup_finish_date
	, s.first_lsn AS first_lsn
	, s.last_lsn AS last_lsn
    , CAST(s.backup_size / 1048576 as INT) as bkSizeInt
    , DATEDIFF(second, s.backup_start_date, s.backup_finish_date) TimeTakenInt
    , CASE s.[type]
        WHEN 'D' THEN '#32FF0000'
        WHEN 'I' THEN '#320AFF0E'
        WHEN 'L' THEN '#32FFFF00'
        ELSE '#00000000'
    END AS rowColor
FROM msdb.dbo.backupset s (NOLOCK)
    INNER JOIN msdb.dbo.backupmediafamily m (NOLOCK) ON s.media_set_id = m.media_set_id
WHERE s.database_name = {1}
ORDER BY backup_start_date DESC
	, backup_finish_date", limit, d.Name);
            }
            return d.ExecuteWithResults(sql);
        }
        #endregion

        #region Audits
        /// <summary>
        /// Get the list of Foreign Keys not indexed. It's often a good idea to index them (not always!)
        /// You can easily customize this query and add/remove columns since Kankuru Datagrid autogenerate columns
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a dataset with the result of the query.</returns>
        public static DataSet GetFKWithoutIndex(this smo.Database d)
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
            return d.ExecuteWithResults(sql);
        }

        /// <summary>
        /// Get duplicated indexes. Duplicated indexes are 2 indexes using the same column.
        /// Inspired by http://blog.developpez.com/sqlpro/p9263/langage-sql-norme/une_requete_recherchant_les_index_anorma
        /// You can easily customize this query and add/remove columns since Kankuru Datagrid autogenerate columns
        /// </summary>
        /// <param name="d">your smo database</param>
        /// <returns>a dataset with the result of the query.</returns>
        public static DataSet GetDuplicatedIndex(this smo.Database d)
        {
            string sql = @"WITH t0 AS (
	SELECT ic.object_id
		, index_id
		, c.column_id
		, key_ordinal
		, CASE is_descending_key WHEN '0' THEN 'asc' WHEN '1' THEN 'desc' END AS orderkey
		, c.name AS column_name
		, ROW_NUMBER() OVER(PARTITION BY ic.object_id, index_id ORDER BY key_ordinal DESC) AS n
	FROM sys.index_columns AS ic  (NOLOCK)
		INNER JOIN sys.columns AS c (NOLOCK) ON ic.object_id = c.object_id
			AND ic.column_id = c.column_id
	WHERE key_ordinal > 0
		AND index_id > 0)
, t1 AS (
	SELECT object_id 
		, index_id 
		, column_id 
		, key_ordinal 
		, n 
		, CAST(column_name as VARCHAR(MAX)) + ' ' + orderkey AS comp_litterale 
		, CAST(column_id as VARCHAR(MAX)) + SUBSTRING(orderkey , 1 , 1) AS comp_math 
		, MAX(n) OVER(PARTITION BY object_id, index_id) AS cmax
	FROM t0 
	WHERE key_ordinal = 1 
	UNION ALL
	SELECT t0.object_id 
		, t0.index_id 
		, t0.column_id 
		, t0.key_ordinal 
		, t0.n 
		, comp_litterale + ', ' + CAST(t0.column_name as VARCHAR(MAX)) + ' ' + t0.orderkey
		, comp_math + CAST(t0.column_id as VARCHAR(MAX)) + SUBSTRING (t0.orderkey , 1 , 1)
		, t1.cmax
	FROM t0 
		INNER JOIN t1 ON t0.object_id = t1.object_id 
			AND t0.index_id = t1.index_id 
			AND t0.key_ordinal = t1.key_ordinal + 1) 
, t2 as (
	SELECT object_id 
		, index_id 
		, comp_litterale 
		, comp_math 
		, cmax 
	FROM t1 
	WHERE n = 1) 
, t4 as (
	SELECT t2.object_id 
		, t2.index_id 
		, t3.index_id as index_id_anomalie 
		, t2.comp_litterale as clef_index 
		, t3.comp_litterale as clef_index_anormal 
		, ABS(t2.cmax - t3.cmax) as distance 
	FROM t2 
		INNER JOIN t2 as t3 ON t2.object_id = t3.object_id 
		AND t2.index_id <> t3.index_id 
		AND t2.comp_math = t3.comp_math) 

SELECT s.name +'.' + o.name as [Table] 
	, i1.name as [Index]
	, i2.name as [Similar Index]
	, t4.clef_index_anormal as [Columns]
FROM t4 
	INNER JOIN sys.objects as o (nolock) ON t4.object_id = o.object_id 
	INNER JOIN sys.schemas as s (nolock) ON o.schema_id = s.schema_id 
	INNER JOIN sys.indexes as i1 (nolock) ON t4.object_id = i1.object_id 
		AND t4.index_id = i1.index_id 
	INNER JOIN sys.indexes as i2 (nolock) ON t4.object_id = i2.object_id 
		AND t4.index_id_anomalie = i2.index_id 
WHERE o.type IN ('u' , 'v') 
ORDER by [Table]
	, [Index]";
            return d.ExecuteWithResults(sql);
        }

        #endregion
    }
}
