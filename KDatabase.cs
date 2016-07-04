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
    }
}
