# KMO - Kankuru Management Object
Kmo is a .net library to extend SMO library. 
SMO is a powerful library provided by Microsoft but in Kankuru I needed more :)
In a near future, I'll extend more methods.

You just need to import KMO.dll in your smo project and you'll obtain more methods.

## C# :

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

## Powershell :

    [System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO')    
    Import-Module "C:\Sources\KMO\bin\Debug\KMO.dll"    
    $header = "-- Stored Procedure Header"    
    $myServer = New-Object ('Microsoft.SqlServer.Management.Smo.Server') "localhost"    
    $myDatabase = $myServer.Databases["AdventureWorks2014"]    
    foreach($myTable in $myDatabase.Tables)    
    {    
        [KMO.KTable]::ScriptProcedureSelect($myTable, $header)    
    }

## Exposed methods

### smo.Database
    IsDistributor()
    
### smo.Table

    ScriptProcedureList(string header)
    ScriptProcedureSelect(string header)
    ScriptProcedureSelectWithTVP(string header)
    ScriptProcedureInsert(string header)
    ScriptProcedureUpdate(string header)
    ScriptProcedureDelete(string header)
    CompareDataLight(smo.Table secondTable)
    CompareData(smo.Table secondTable, out DataTable RowsAdded, out DataTable RowsDeleted, out DataTable RowsUpdated, [bool details = false])
    

### smo.DataType

    ScriptToSql() to convert a DataType into TSql
