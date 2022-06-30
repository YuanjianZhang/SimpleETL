using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.MySQL.EntityFrameworkCore.Model
{
    [Obsolete("不再使用EF框架获取TableSchema ", true)]
    [Keyless]
    public class M_TableSchema_MySQL
    {
        public string? Field { get; set; }
        public string? Type { get; set; }
        public string? Collation { get; set; }
        public string? Null { get; set; }
        public string? Key { get; set; }
        public string? Default { get; set; }
        public string? Extra { get; set; }
        public string? Privileges { get; set; }
        public string? Comment { get; set; }
    }
}
