using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using smo = Microsoft.SqlServer.Management.Smo;

namespace KMO
{
    public static class KView
    {
        /// <summary>
        /// Compare 2 ddl Views
        /// </summary>
        /// <param name="sp1">your first smo view</param>
        /// <param name="sp2">your second smo view</param>
        /// <param name="checkComments">True if you want to compare comments</param>
        /// <param name="checkBrackets">True if you want to compare scripts with brackets</param>
        /// <param name="ignoreCaseSensitive">True if you want to ignore Case Sensitive. False if Case sensitive</param>
        /// <returns></returns>
        public static KMOCompareInfo CompareSchema(this smo.View v1, smo.View v2, bool checkComments = false, bool checkBrackets = false, bool ignoreCaseSensitive = true)
        {
            smo.ScriptingOptions so = new smo.ScriptingOptions();
            so.DriAll = true;
            string s1 = String.Join(Environment.NewLine, v1.Script(so).Cast<String>().Select(s => s.ToString()).AsEnumerable());
            string s2 = String.Join(Environment.NewLine, v2.Script(so).Cast<String>().Select(s => s.ToString()).AsEnumerable());
            string message = string.Empty;
            if (KMOCompareHelper.CompareScript(s1, s2, ignoreCaseSensitive, checkComments, checkBrackets))
            {
                return new KMOCompareInfo { IsIdentical = true, Message = string.Empty, Script1 = s1, Script2 = s2 };
            }
            else
            {
                return new KMOCompareInfo { IsIdentical = false, Message = "Script difference", Script1 = s1, Script2 = s2 };
            }
        }
    }
}


//public static class KStoredProcedure
//{
//    public static KMOCompareInfo CompareSchema(this smo.StoredProcedure sp1, smo.StoredProcedure sp2, bool checkComments = false, bool checkBrackets = false, bool ignoreCaseSensitive = true)
//    {
//        smo.ScriptingOptions so = new smo.ScriptingOptions();
//        so.DriAll = true;
//        string s1 = String.Join(Environment.NewLine, sp1.Script(so).Cast<String>().Select(s => s.ToString()).AsEnumerable());
//        string s2 = String.Join(Environment.NewLine, sp2.Script(so).Cast<String>().Select(s => s.ToString()).AsEnumerable());
//        string message = string.Empty;
//        if (KMOCompareHelper.CompareScript(s1, s2, ignoreCaseSensitive, checkComments, checkBrackets))
//        {
//            return new KMOCompareInfo { IsIdentical = true, Message = string.Empty, Script1 = s1, Script2 = s2 };
//        }
//        else
//        {
//            return new KMOCompareInfo { IsIdentical = false, Message = "Script difference", Script1 = s1, Script2 = s2 };
//        }
//    }
//}

