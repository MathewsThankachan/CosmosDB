using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
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


        static StorageCredentials storageCredentials;
        static StorageUri storageUri;
        static CloudStorageAccount storageAccount;
        static CloudBlobClient cloudBlobClient;

        static BlobContainerClient blobContainerClient;
        static CloudBlobContainer cloudBlobContainer;
        static CloudBlobContainer cloudBlobPricingContainer;
        static CloudBlob cloudBlob;
        static CloudBlockBlob blockBlob;

        static string CosmosDBEndpointUrl;
        static string CosmosDBAuthorizationKey;
        static string StorageAccountName;
        static string StorageAccountKey;
        static string StorageContainerName;
        static string StoragePricingContainerName;
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
            StoragePricingContainerName = config.GetSection("Dev_Storage")["StoragePricingContainerName"];


            StorageConnectionString = config.GetSection("Dev_Storage")["StorageConnectionString"];

            SplitFiles = bool.Parse(config.GetSection("SourceFile")["SplitFiles"]) || false;
            FileMaxRows = int.Parse(config.GetSection("SourceFile")["FileMaxRows"]);
            FileName = config.GetSection("SourceFile")["FileName"];

            storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);
            storageUri = new StorageUri(new Uri(StorageEndpointUrl));
            storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            cloudBlobClient = storageAccount.CreateCloudBlobClient();
            cloudBlobContainer = cloudBlobClient.GetContainerReference(StorageContainerName);
            cloudBlobPricingContainer = cloudBlobClient.GetContainerReference(StoragePricingContainerName);
            blobContainerClient = new BlobContainerClient(StorageConnectionString, StoragePricingContainerName);

            cloudBlob = cloudBlobContainer.GetBlobReference("mergedfiles.txt "); //2019_PricePaidData.txt //not used


        }
        public async static void SplitStorageFile()
        {
            Intialiaze();


            blockBlob = cloudBlobContainer.GetBlockBlobReference("2019_PricePaidData.txt"); //mergedfiles.txt

            using (var fileStream = System.IO.File.OpenWrite("myfile.txt"))
            {
                await blockBlob.DownloadToStreamAsync(fileStream);
            }


            int maxRowCount = 1;
            int fileNumberCount = 1;
            FileName = FileName + DateTime.Now.ToString("dd-MMM-yyyy") + "_";


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
                            await writer.WriteLineAsync(line);
                            //writeline takes 7:03 am and end at 10:38 am for 570 mb/ 3,459340
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

        public static List<string> GetStorageFiles()
        {
            Intialiaze();

            List<string> lstFileName = new List<string>();
            foreach (BlobItem blobItem in blobContainerClient.GetBlobs())
            {
                if (blobItem.Name.ToLower() != "ok.txt") // send the list of files except the ok file.
                {
                    lstFileName.Add(blobItem.Name);
                }
            }

            return lstFileName;
        }

        public static async Task DownloadFileAsync(string fileName)
        {
            blockBlob = cloudBlobContainer.GetBlockBlobReference(fileName);

            using (var fileStream = System.IO.File.OpenWrite(fileName))
            {
                await blockBlob.DownloadToStreamAsync(fileStream);
            }
            
        }
    }
}
