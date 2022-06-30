using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.DB.Common
{
    public class DBCommon : IDBCommon
    {
        private readonly IConfiguration _configure;
        public DBCommon(IConfiguration configure) => _configure = configure;

        public string GetConnectionString(string Key)
        {
            return _configure.GetConnectionString(Key);
        }

    }
}
