using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.Common
{
    public interface IDBCommon
    {
        public string GetConnectionString(string Key);
    }
}
