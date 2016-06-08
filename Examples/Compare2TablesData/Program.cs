using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using KMO;
using System.Data;

namespace Compare2TablesData
{
    class Program
    {
        static void Main(string[] args)
        {
            ServerConnection myCnx1 = new ServerConnection("localhost"); // connection string
            Server myServer1 = new Server(myCnx1);
            Database myDatabase1 = myServer1.Databases["AdventureWorks2014"]; // your database
            Table t1 = myDatabase1.Tables["FirstTable", "dbo"]; // your first table

            ServerConnection myCnx2 = new ServerConnection("localhost"); // connection string
            Server myServer2 = new Server(myCnx2);
            Database myDatabase2 = myServer2.Databases["AdventureWorks2014"]; // your database
            Table t2 = myDatabase2.Tables["SecondTable", "dbo"]; // your second table

            // light compare
            Console.WriteLine(string.Format("Identical data : {0}", t1.CompareDataLight(t2))); 

            // or compare with more informations
            DataTable dtRowsAdded, dtRowsDeleted, dtRowsUpdated;
            Console.WriteLine(string.Format("Identical data : {0}", t1.CompareData(t2, out dtRowsAdded, out dtRowsDeleted, out dtRowsUpdated)));
            if (dtRowsAdded != null)
            {
                Console.WriteLine("Added : " + dtRowsAdded.Rows.Count.ToString());
            }
            if (dtRowsDeleted != null)
            {
                Console.WriteLine("Deleted : " + dtRowsDeleted.Rows.Count.ToString());
            }
            if (dtRowsUpdated != null)
            {
                Console.WriteLine("Updated : " + dtRowsUpdated.Rows.Count.ToString());
            }

            myCnx1.Disconnect();
            myCnx2.Disconnect();
            Console.Read();

        }
    }
}
