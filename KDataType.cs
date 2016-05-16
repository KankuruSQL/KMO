using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
namespace KMO
{
    /// <summary>
    /// Microsoft.SqlServer.Management.Smo.DataType extension class
    /// </summary>
    public static class KDataType
    {
        /// <summary>
        /// Transform a DataType into SQL
        /// </summary>
        /// <param name="dType"></param>
        /// <returns>a ddl string</returns>
        public static string ScriptToSql(this DataType dType)
        {
            string sqlType = string.Empty;
            switch (dType.SqlDataType.ToString())
	        {
		        case "BigInt":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Binary":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "Bit":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Char":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "Date":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "DateTime":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "DateTime2":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "DateTimeOffset":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "Decimal":
                    sqlType = string.Format("{0}({1}, {2})", dType.SqlDataType.ToString() , dType.NumericPrecision.ToString(), dType.NumericScale.ToString());
                    break;
                case "Float":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "Geography":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Geometry":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "HierarchyId":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Image":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Int":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Money":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "NChar":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "NText":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Numeric":
                    sqlType = string.Format("{0}({1}, {2})", dType.SqlDataType.ToString() , dType.NumericPrecision.ToString(), dType.NumericScale.ToString());
                    break;
                case "NVarChar":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "NVarCharMax":
                    sqlType = "NVarChar(MAX)";
                    break;
                case "Real":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "SmallDateTime":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "SmallInt":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "SmallMoney":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "SysName":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Text":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "Time":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "Timestamp":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "TinyInt":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "UniqueIdentifier":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "VarBinary":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "VarBinaryMax":
                    sqlType = "VarBinary(MAX)";
                    break;
                case "VarChar":
                    sqlType = string.Format("{0}({1})", dType.SqlDataType.ToString(), dType.MaximumLength.ToString());
                    break;
                case "VarCharMax":
                    sqlType = "VarChar(MAX)";
                    break;
                case "Xml":
                    sqlType = dType.SqlDataType.ToString();
                    break;
                case "UserDefinedDataType":
                    sqlType = dType.Name;
                    break;
                default:
                    sqlType = dType.SqlDataType.ToString();
                    break;
	        }
            return sqlType;           
        }
    }
}
