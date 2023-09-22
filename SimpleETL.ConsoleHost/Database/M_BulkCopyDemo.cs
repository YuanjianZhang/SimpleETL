using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleETL.ConsoleHost.Database
{
    [Table("BULKCOPYDEMO")]
    public class M_BulkCopyDemo
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int ID { get; set; }
        [Column(TypeName ="varchar(500)")]
        public string NAME { get; set; }
        [Column(TypeName ="varchar(500)")]
        public string CONTEXT { get; set; }
        [Column(TypeName="varchar(100)")]
        public string CREATETIME { get; set; }
        public long CREATETICKS { get; set; }
    }
}
