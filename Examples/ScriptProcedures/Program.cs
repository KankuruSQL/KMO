using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using KMO;

namespace ScriptProcedures
{
    class Program
    {
        static void Main(string[] args)
        {
            string header = "-- Stored Procedure Header";
            ServerConnection myCnx = new ServerConnection("localhost"); // connection string
            Server myServer = new Server(myCnx);
            Database myDatabase = myServer.Databases["AdventureWorks2014"]; // your database
            foreach (Table myTable in myDatabase.Tables)
            {
                Console.WriteLine(myTable.Name);
                Console.WriteLine(myTable.ScriptProcedureList(header)); // Script Select whole table procedure
                Console.WriteLine(myTable.ScriptProcedureSelect(header)); // Script Select procedure
                Console.WriteLine(myTable.ScriptProcedureSelectWithTVP(header)); // Script Select procedure with table value parameter
                Console.WriteLine(myTable.ScriptProcedureInsert()); // Without header. Script insert procedure
                Console.WriteLine(myTable.ScriptProcedureUpdate("/* Update procedure header! */")); // with a different header. Script update procedure
                Console.WriteLine(myTable.ScriptProcedureDelete(header)); // Script Delete procedure
            }
            myCnx.Disconnect();
            Console.Read();
        }
    }
}
