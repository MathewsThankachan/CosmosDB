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
using System.Text.Json;
using System.Threading.Tasks;


namespace FunctionApp
{
    public static class SplitStorageFiles
    {


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


        private static void Intialiaze()
        {
            var config = new ConfigurationBuilder()
                    .SetBasePath(System.Environment.CurrentDirectory)
                    .AddJsonFile("appconfig.settings.json", optional: false, reloadOnChange: true)
                    .Build();



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
            Intialiaze();

            StorageCredentials storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);
            StorageUri storageUri = new StorageUri(new Uri(StorageEndpointUrl));
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
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

    }
}
