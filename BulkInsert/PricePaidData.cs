using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{

    public class PricePaidData
    {

        [JsonProperty(PropertyName = "id")]
        public string Transaction_unique_identifieroperty { get; set; }
        public string Price { get; set; }

        public string Date_of_Transfer { get; set; }
        public string Postcode { get; set; }
        public string PropertyType { get; set; }
        public string isNew { get; set; }
        public string Duration { get; set; }
        public string PAON { get; set; }
        public string SAON { get; set; }
        public string Street { get; set; }
        public string Locality { get; set; }
        public string Town_City { get; set; }

        public string District { get; set; }
        public string County { get; set; }
        public string PPD_Category{ get; set; }
        public string Record_Status { get; set; }

    }
}
