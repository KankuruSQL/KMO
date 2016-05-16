# KMO
Kankuru Management Object
Kmo is a .net library to extend SMO library. 
SMO is a powerful library provided by Microsoft but in Kankuru I needed more :)
In a near future, I'll extend more methods.

You just need to import KMO.dll in your smo project and you'll obtain more methods.

Example :
You can use KMO with SMO in C# :
using Microsoft.SqlServer.Management.Smo;
using KMO;
namespace KMOTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string header = "-- Stored Procedure Header";
            Server myServer = new Server("localhost"); // connection string
            Database myDatabase = myServer.Databases["AdventureWorks2014"]; // your database
            foreach (Table myTable in myDatabase.Tables)
            {
                Console.WriteLine(myTable.ScriptProcedureSelect(header)); // here is one KMO method
            }
        }
    }
}

Or in powershell :
[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO')
Import-Module "C:\Sources\KMO\bin\Debug\KMO.dll"
$header = "-- Stored Procedure Header"
$myServer = New-Object ('Microsoft.SqlServer.Management.Smo.Server') "localhost"
$myDatabase = $myServer.Databases["AdventureWorks2014"]
foreach($myTable in $myDatabase.Tables)
{
    [KMO.KTable]::ScriptProcedureSelect($myTable, $header)
}
