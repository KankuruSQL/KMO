using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using smo = Microsoft.SqlServer.Management.Smo;

namespace KMO
{
    public static class KServer
    {
        /// <summary>
        /// Get Sql server version name from major and minor version
        /// </summary>
        /// <param name="s">your smo server object</param>
        /// <returns>a string</returns>
        public static string VersionString(this smo.Server s)
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
            return string.Format("{0} {1} {2} ({3})", s.VersionString(), s.ProductLevel, s.Edition, s.Version);
        }

    }
}
