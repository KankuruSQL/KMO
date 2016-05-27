using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using KMO;

namespace GetDistributionDatabases
{
    class Program
    {
        static void Main(string[] args)
        {
            ServerConnection myCnx = new ServerConnection("localhost"); // connection string
            Server myServer = new Server(myCnx);
            IEnumerable<Database> distribDbs = myServer.Databases.Cast<Database>().Where(d => d.IsDistributor());            
            foreach (Database db in distribDbs)
            {
                Console.WriteLine(db.Name);
            }
            Console.Read();
        }
    }
}
