using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.SQL.EntityFrameworkCore.Model
{
    [Obsolete("不再使用EF框架获取TableSchema ", true)]
    [Keyless]
    public class M_TableSchema_SQL
    {
        public string? TABLE_CATALOG { get; set; }
        public string? TABLE_SCHEMA { get; set; }
        public string? TABLE_NAME { get; set; }
        public string? COLUMN_NAME { get; set; }
        public int? ORDINAL_POSITION { get; set; }
        public string? COLUMN_DEFAULT { get; set; }
        public string? IS_NULLABLE { get; set; }
        public string? DATA_TYPE { get; set; }
        public int? CHARACTER_MAXIMUM_LENGTH { get; set; }
        public int? CHARACTER_OCTET_LENGTH { get; set; }
        public byte? NUMERIC_PRECISION { get; set; }
        public short? NUMERIC_PRECISION_RADIX { get; set; }
        public int? NUMERIC_SCALE { get; set; }
        public short? DATETIME_PRECISION { get; set; }
        public string? CHARACTER_SET_CATALOG { get; set; }
        public string? CHARACTER_SET_SCHEMA { get; set; }
        public string? CHARACTER_SET_NAME { get; set; }
        public string? COLLATION_CATALOG { get; set; }
    }
}
