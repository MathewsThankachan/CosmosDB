using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace FunctionApp
{
    public static class blobTriggerCheckOkFile
    {

        [FunctionName("blobTriggerCheckOkFile")]
        public async static void Run([BlobTrigger("pricing/{name}", Connection = "StorageConnectionString")]Stream myBlob, string name, ILogger log)
        {

            string uploadedFileName = name;
            List<string> lstFileNames = new List<string>();

            if (name.ToLower() == "ok.txt") //look for ok file on the pricing container
            {
                //Write code to list the contents of the storage and download them one by one


                //Move the storage files to the Staging folder of the container

                lstFileNames = SplitStorageFiles.GetStorageFiles();



            }

            foreach (var fileName in lstFileNames)
            {

                //await SplitStorageFiles.DownloadFileAsync(fileName);
            }

            SplitStorageFiles.SplitStorageFile();

        }

    }



}
