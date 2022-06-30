using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.VisualStudio.TestTools.UnitTesting.Logging;
using SimpleETL.Model;
using System.Linq;
using System;

namespace SimpleETL.Test
{
    [TestClass]
    public class EFCoreTest
    {
        private static string DBString = "Data Source=192.168.1.60;uid=sa;pwd=yilianwang;database=plat_lis;";
        [TestMethod]
        public void TestDB()
        {
            try
            {
                using var db = new PLAT_LISContext();
                db.DbPath = DBString;
                Logger.LogMessage($"Database path: {db.DbPath}.");

                // Create
                Logger.LogMessage("Inserting a new blog");
                db.Add(new M_Lis_ReportIndex { RDN = "521541" });
                db.SaveChanges();

                // Read
                Logger.LogMessage("Querying for a blog");
                var blog = db.Lis_ReportIndex.Where(p => p.RDN == "521541").OrderBy(p => p.RDN).First();

                // Update
                Logger.LogMessage("Updating the blog and adding a post");
                blog.JYLSH = "546244515442514";
                db.SaveChanges();

                // Delete
                Logger.LogMessage("Delete the blog");
                db.Remove(blog);
                db.SaveChanges();
            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex.Message);
                Assert.Fail();
            }
            
        }
    }
}