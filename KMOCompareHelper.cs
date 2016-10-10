using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace KMO
{
    static class KMOCompareHelper
    {
        /// <summary>
        /// Compare 2 T-Sql Scripts
        /// </summary>
        /// <param name="s1">your first script</param>
        /// <param name="s2">your second script</param>
        /// <param name="ignoreCaseSensitive">False if you want to compare 2 scripts with Case Sensitive</param>
        /// <param name="checkComments">True if you want to include comments in the comparison</param>
        /// <param name="checkBrackets">True if you want to include brackets in the comparison</param>
        /// <returns></returns>
        public static bool CompareScript(string s1, string s2, bool ignoreCaseSensitive, bool checkComments, bool checkBrackets)
        {
            if (!checkComments)
            {
                // remove tsql comments
                s1 = Regex.Replace(s1, @"(?s)/\*.*?\*/", "");
                s2 = Regex.Replace(s2, @"(?s)/\*.*?\*/", "");
                s1 = Regex.Replace(s1, @"--(.*?)\r\n", "\r\n");
                s2 = Regex.Replace(s2, @"--(.*?)\r\n", "\r\n");
                s1 = Regex.Replace(s1, @"--(.*?)\n", "\n");
                s2 = Regex.Replace(s2, @"--(.*?)\n", "\n");
            }
            if (!checkBrackets)
            {
                // remove brackets
                s1 = s1.Replace("[", "").Replace("]", "");
                s2 = s2.Replace("[", "").Replace("]", "");
            }
            if (String.Compare(s1, s2, ignoreCaseSensitive) == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
