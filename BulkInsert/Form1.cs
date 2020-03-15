
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Windows.Forms;
using Container = Microsoft.Azure.Cosmos.Container;
using Microsoft.Azure.Cosmos;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;

namespace BulkInsert
{
    public partial class Form1 : Form
    {
        string EndpointUrl = ConfigurationManager.AppSettings["EndpointUrl"];
        string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        string ContainerName = ConfigurationManager.AppSettings["ContainerName"];
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private async void btnBulkInsert_Click(object sender, EventArgs e)
        {
            btnBulkInsert.Enabled = false;
            string filePath = Path.GetDirectoryName(Application.ExecutablePath).Replace(@"bin\Debug", ConfigurationManager.AppSettings["FilePath"]);

            List<PricePaidData> lstPricedata = new List<PricePaidData>();
            Stopwatch sw = new Stopwatch();

            Console.WriteLine("Reading File and appending to List : " + DateTime.Now.ToString("dd--MMM-yyyy hh:mm:ss"));
            lstPricedata = readfile(filePath);

            Console.WriteLine("Total Time reading the file :  " + sw.ElapsedMilliseconds.ToString());
            sw.Restart();

            // 500k --> 15 min @ 20,000RU/s, && 11 mins  @ 100,000RU/s
            // 1 mn -->  min @ 100,000RU/s

            //Create a new collection to test various range of data. This test code and needs to be removed
            List<PricePaidData> lstPricedata1 = lstPricedata.GetRange(0, 500000);


            sw.Start();// initialize the stop watch for testing the throughput of Cosmosdb SQL API


            BulkInsertCreateAsyncMethod(lstPricedata1);



            btnBulkInsert.Enabled = true;
            Console.WriteLine("Total Time writing to CosmosDB  :  " + (sw.ElapsedMilliseconds / 1000).ToString() + "  seconds");
            //MessageBox.Show("Total Time writing to CosmosDB  :  " + (sw.ElapsedMilliseconds / 1000).ToString() + "  seconds");
            sw.Stop();


        }

        //Method for Bulk insert records using createAsync method
        public async void BulkInsertCreateAsyncMethod(List<PricePaidData> lstPricedata)
        {

            //Define the conectivity option to cosmos client
            CosmosClientOptions options = new CosmosClientOptions()
            {
                AllowBulkExecution = true,
                ConnectionMode = ConnectionMode.Direct,
                MaxRequestsPerTcpConnection = -1,
                MaxTcpConnectionsPerEndpoint = -1,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryAttemptsOnRateLimitedRequests = 999,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromHours(1),
            };

            #region CosmosDB Connection settings

            CosmosClient client = new CosmosClient(EndpointUrl, AuthorizationKey, options);
            Database database = await client.CreateDatabaseIfNotExistsAsync(DatabaseName);
            //Container container = await database.CreateContainerIfNotExistsAsync(ContainerName, "/Postcode");
            Container container = await database.DefineContainer(ContainerName, "/Postcode").WithIndexingPolicy()
                .WithIndexingMode(IndexingMode.Consistent).WithIncludedPaths().Attach()
                .WithExcludedPaths().Path("/*").Attach().Attach()
                .CreateIfNotExistsAsync(5000);
            

            //database.ReplaceThroughputAsync()

            //ThroughputResponse throughput = await container.ReplaceThroughputAsync(20000);
            #endregion
            int cnt = 0;
            List<Task> tasks = new List<Task>();



            foreach (var item in lstPricedata)
            {
                cnt++; // only used for debugging to see current record index being processed
                tasks.Add(container.CreateItemAsync<PricePaidData>(item, new PartitionKey(item.Postcode)));
            }

            await Task.WhenAll(tasks);

            //throughput = await container.ReplaceThroughputAsync(400);
        }




        private void btnBlobStorageBulkInsert_Click(object sender, EventArgs e)
        {
            btnBlobStorageBulkInsert.Enabled = false;

            string StorageEndpointUrl = ConfigurationManager.AppSettings["StorageEndpointUrl"];
            string StorageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
            string StorageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"];
            string StorageContainerName = ConfigurationManager.AppSettings["StorageContainerName"];

            StorageCredentials storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);

            CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, true);
            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(StorageContainerName);
            CloudBlob cloudBlob = cloudBlobContainer.GetBlobReference("2019_PricePaidData.txt");


            List<PricePaidData> lstPricedata = new List<PricePaidData>();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            using (var stream = cloudBlob.OpenRead())
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine();

                        lstPricedata.Add(PopulatePriceData(line));
                    }
                }
            }

            //reading file from local to azure takes approx 53 seconds
            //MessageBox.Show("Time Taken to read the file :" + (sw.ElapsedMilliseconds / 1000).ToString() + "\n Number of line : " + lstPricedata.Count.ToString());

            sw.Restart(); //restarting the stop watch to calculate the time taken for bulk insert record only

            //Create a new collection to test various range of data. This test code and needs to be removed
            //List<PricePaidData> lstPricedata1 = lstPricedata.GetRange(0, lstPricedata.Count);
            List<PricePaidData> lstPricedata1 = lstPricedata.GetRange(0, 5000);


            BulkInsertCreateAsyncMethod(lstPricedata1);


            btnBlobStorageBulkInsert.Enabled = true;
            Console.WriteLine("Total Time writing to CosmosDB  :  " + (sw.ElapsedMilliseconds / 1000).ToString() + "  seconds");
            //MessageBox.Show("Total Time writing to CosmosDB  :  " + (sw.ElapsedMilliseconds / 1000).ToString() + "  seconds");
            sw.Stop();

        }



        public void BulkInsertCreateAsyncStreamWriterMethod(List<PricePaidData> lstPricedata)
        {
            #region CosmosDb StreamWriter --> better performance
            //foreach (var item in lstPricedata1)
            //{

            //    cnt++;
            //    MemoryStream ms = new MemoryStream();
            //    JsonSerializer Serializer = new JsonSerializer();
            //    using (StreamWriter streamwriter = new StreamWriter(ms, Encoding.UTF8))
            //    {
            //        using (JsonWriter writer = new JsonTextWriter(streamwriter))
            //        {
            //            writer.Formatting = Newtonsoft.Json.Formatting.None;
            //            Serializer.Serialize(writer, item);
            //            writer.Flush();
            //            streamwriter.Flush();
            //            ms.Position = 0;
            //            ResponseMessage response = await container.CreateItemStreamAsync(ms, new PartitionKey(item.Postcode));
            //            if (response.IsSuccessStatusCode)
            //                successCnt++;

            //        }

            //    }
            //}

            #endregion
        }


        private List<PricePaidData> readfile(string filePath)
        {
            List<PricePaidData> lstPricedata = new List<PricePaidData>();
            using (FileStream fs = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (BufferedStream bs = new BufferedStream(fs))
                {
                    using (StreamReader sr = new StreamReader(bs))
                    {
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {

                            lstPricedata.Add(PopulatePriceData(line));
                        }
                    }
                }
            }
            return lstPricedata;
        }

        private PricePaidData PopulatePriceData(string line)
        {

            string[] pricedataArray = line.Split(',');
            PricePaidData ppd = new PricePaidData()
            {
                Transaction_unique_identifieroperty = pricedataArray[0],
                Price = pricedataArray[1].Replace("\"", ""),
                Date_of_Transfer = pricedataArray[2].Replace("\"", ""),
                Postcode = pricedataArray[3].Replace("\"", ""),
                PropertyType = pricedataArray[4].Replace("\"", ""),
                isNew = pricedataArray[5].Replace("\"", ""),
                Duration = pricedataArray[6].Replace("\"", ""),
                PAON = pricedataArray[7].Replace("\"", ""),
                SAON = pricedataArray[8].Replace("\"", ""),
                Street = pricedataArray[9].Replace("\"", ""),
                Locality = pricedataArray[10].Replace("\"", ""),
                Town_City = pricedataArray[11].Replace("\"", ""),
                District = pricedataArray[12].Replace("\"", ""),
                County = pricedataArray[13].Replace("\"", ""),
                PPD_Category = pricedataArray[14].Replace("\"", ""),
                Record_Status = pricedataArray[15].Replace("\"", "")

            };
            return ppd;
        }
    }
}

