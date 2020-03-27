using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace FunctionApp
{
    public static class BulkInsert
    {

        static int TimeToLive;
        static string CosmosDBEndpointUrl;
        static string CosmosDBAuthorizationKey;
        static string StorageAccountName;
        static string StorageAccountKey;
        static string StorageContainerName;
        static string StorageEndpointUrl;
        static string DatabaseName;
        static string ContainerName;
        static string StorageConnectionString;
        static string CosmosDBConnectionString;

        static bool SplitFiles;
        static int? FileMaxRows;
        static string FileName;

        [FunctionName("BulkInsert")]
        public static void Run([TimerTrigger("0 * 23  * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            Intialiaze();

            ReadFileFromStorage();

            //SplitStorageFile();
        }


        private static void Intialiaze()
        {
            var config = new ConfigurationBuilder()
                    .SetBasePath(System.Environment.CurrentDirectory)
                    .AddJsonFile("appconfig.settings.json", optional: false, reloadOnChange: true)
                    .Build();


            TimeToLive = Int32.Parse(config["TimeToLive"]);

            CosmosDBEndpointUrl = config.GetSection("CosmosDB")["CosmosDBEndpointUrl"];
            CosmosDBAuthorizationKey = config.GetSection("Dev_CosmosDB")["CosmosDBAuthorizationKey"];
            DatabaseName = config.GetSection("Dev_CosmosDB")["DatabaseName"];
            ContainerName = config.GetSection("Dev_CosmosDB")["ContainerName"];
            CosmosDBConnectionString = config.GetSection("Dev_CosmosDB")["CosmosDBConnectionString"];
            StorageEndpointUrl = config.GetSection("Dev_Storage")["StorageEndpointUrl"];
            StorageAccountName = config.GetSection("Dev_Storage")["StorageAccountName"];
            StorageAccountKey = config.GetSection("Dev_Storage")["StorageAccountKey"];
            StorageContainerName = config.GetSection("Dev_Storage")["StorageContainerName"];

            StorageConnectionString = config.GetSection("Dev_Storage")["StorageConnectionString"];

            SplitFiles = bool.Parse(config.GetSection("SourceFile")["SplitFiles"]) || false;
            FileMaxRows = int.Parse(config.GetSection("SourceFile")["FileMaxRows"]);
            FileName = config.GetSection("SourceFile")["FileName"];
        }


        public async static void SplitStorageFile()
        {
            StorageCredentials storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);
            StorageUri storageUri = new StorageUri(new Uri(StorageEndpointUrl));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(StorageConnectionString);//= new CloudStorageAccount(storageCredentials,true);
            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(StorageContainerName);
            CloudBlob cloudBlob = cloudBlobContainer.GetBlobReference("2019_PricePaidData.txt");
            CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference("2019_PricePaidData.txt");

            int maxRowCount = 1;
            int fileNumberCount = 1;
            FileName = FileName + DateTime.Now.ToString("dd-MMM-yyyy") + "_";

            using (var fileStream = System.IO.File.OpenWrite("myfile.txt"))
            {
                await blockBlob.DownloadToStreamAsync(fileStream);
            }
            using (StreamReader reader = new StreamReader("myfile.txt"))
            {
                while (!reader.EndOfStream)
                {
                    if (FileMaxRows >= maxRowCount)
                    {
                        maxRowCount++;
                        string line = reader.ReadLine();

                        using (StreamWriter writer = new StreamWriter(FileName + fileNumberCount + ".txt", append: true))
                        {
                            writer.AutoFlush = true;
                            writer.WriteLine(line);

                        }
                    }
                    else
                    {
                        string filename = FileName + fileNumberCount + ".txt";
                        await cloudBlobContainer.GetBlockBlobReference(filename).UploadFromFileAsync(filename);
                        fileNumberCount++;
                        maxRowCount = 1;
                    }
                }
            }

        }

        private async static void ReadFileFromStorage()
        {

            StorageCredentials storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);
            StorageUri storageUri = new StorageUri(new Uri(StorageEndpointUrl));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(StorageConnectionString);


            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(StorageContainerName);
            CloudBlob cloudBlob = cloudBlobContainer.GetBlobReference("2019_PricePaidData.txt");

            List<PricePaidData> lstPricedata = new List<PricePaidData>();

            using (var stream = await cloudBlob.OpenReadAsync())
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine().Replace("\"", "");
                        lstPricedata.Add(PopulatePriceData(line));
                    }
                }
            }

            BulkInsertCreateAsyncMethod(lstPricedata);
        }

        private static void Createfile(ref CloudBlobContainer cloudBlobContainer, string DestinationFileName, List<string> lstLine)
        {
            CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(DestinationFileName);
            blockBlob.PutBlockListAsync(lstLine);



        }

        private static List<PricePaidData> readfile(string filePath)
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

        private static PricePaidData PopulatePriceData(string line)
        {

            string[] pricedataArray = line.Split(',');
            PricePaidData ppd = new PricePaidData()
            {
                ID = Guid.NewGuid(),
                Transaction_unique_identifieroperty = pricedataArray[0],
                Price = pricedataArray[1],
                Date_of_Transfer = pricedataArray[2],
                Postcode = pricedataArray[3],
                PropertyType = pricedataArray[4],
                isNew = pricedataArray[5],
                Duration = pricedataArray[6],
                PAON = pricedataArray[7],
                SAON = pricedataArray[8],
                Street = pricedataArray[9],
                Locality = pricedataArray[10],
                Town_City = pricedataArray[11],
                District = pricedataArray[12],
                County = pricedataArray[13],
                PPD_Category = pricedataArray[14],
                Record_Status = pricedataArray[15],
                //setting ttl for each record
                //TimeToLive = 120

            };
            return ppd;
        }


        //Method for Bulk insert records using createAsync method
        public async static void BulkInsertCreateAsyncMethod(List<PricePaidData> lstPricedata)
        {

            //Define the conectivity option to cosmos client
            CosmosClientOptions options = new CosmosClientOptions()
            {
                AllowBulkExecution = true,
                ConnectionMode = Microsoft.Azure.Cosmos.ConnectionMode.Direct,
                MaxRequestsPerTcpConnection = 1000,
                MaxRetryAttemptsOnRateLimitedRequests = 100,
                RequestTimeout = new TimeSpan(0, 5, 0),
                MaxTcpConnectionsPerEndpoint = 1000,
                OpenTcpConnectionTimeout = new TimeSpan(0, 5, 0),
            };

            #region CosmosDB Connection settings

            //CosmosClient client = new CosmosClient(CosmosDBEndpointUrl, CosmosDBAuthorizationKey, options);
            CosmosClient client = new CosmosClient(CosmosDBConnectionString, options);
            Database database = await client.CreateDatabaseIfNotExistsAsync(DatabaseName);
            Container container = await database.DefineContainer(ContainerName, "/Postcode") ///County"
                    .WithIndexingPolicy()
                        .WithIndexingMode(IndexingMode.Consistent)
                        .WithIncludedPaths()
                            .Attach()
                        .WithExcludedPaths()
                            .Path("/*")
                            .Attach()
                    .Attach()
                   .WithDefaultTimeToLive(TimeToLive)
                .CreateIfNotExistsAsync();


            //Container container = await database.CreateContainerIfNotExistsAsync(ContainerName, "/County");

            //database.ReplaceThroughputAsync()

            //ThroughputResponse throughput = await container.ReplaceThroughputAsync(20000);
            #endregion
            int cnt = 0;
            List<Task> tasks = new List<Task>();



            foreach (var item in lstPricedata.Take(100000))
            {
                cnt++; // only used for debugging to see current record index being processed
                //tasks.Add(container.CreateItemAsync<PricePaidData>(item, new PartitionKey(item.County)));


                tasks.Add(
                    container.CreateItemAsync<PricePaidData>(item, new PartitionKey(item.Postcode))
                    .ContinueWith((Task<ItemResponse<PricePaidData>> task) =>
                    {
                        if (!task.IsCompletedSuccessfully)
                        {
                            AggregateException innerExceptions = task.Exception.Flatten();
                            CosmosException cosmosException = innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) as CosmosException;
                            // Console.WriteLine($"Item {item.Transaction_unique_identifieroperty} failed with status code {cosmosException.StatusCode}");
                            //write to the log file for exception records
                        }
                    }));
            }

            await Task.WhenAll(tasks);

            //throughput = await container.ReplaceThroughputAsync(400);
        }



    }
}
