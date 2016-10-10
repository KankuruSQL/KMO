using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KMO
{
    /// <summary>
    /// This object is used to compare 2 objects. 2 tables for example
    /// </summary>
    public class KMOCompareInfo
    {
        public bool IsIdentical { get; set; }
        public string Message { get; set; }
        public string Script1 { get; set; }
        public string Script2 { get; set; }
    }
}
