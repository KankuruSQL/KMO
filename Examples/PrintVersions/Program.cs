using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using KMO;

namespace PrintVersions
{
    class Program
    {
        static void Main(string[] args)
        {
            // In this example, you can get your instance version
            ServerConnection myCnx = new ServerConnection("localhost");
            Server myServer = new Server(myCnx);
            Console.WriteLine("Version : " + myServer.VersionName());
            Console.WriteLine("Full Version : " + myServer.VersionFull());
            myCnx.Disconnect();
            Console.Read();
        }
    }
}
