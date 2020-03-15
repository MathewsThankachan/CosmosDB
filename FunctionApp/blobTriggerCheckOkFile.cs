using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace FunctionApp
{
    public static class blobTriggerCheckOkFile
    {

        [FunctionName("blobTriggerCheckOkFile")]
        public static void Run([BlobTrigger("pricing/{name}", Connection = "StorageConnectionString")]Stream myBlob, string name, ILogger log)
        {

            string uploadedFileName = name ;

            if (name.ToLower()=="ok.txt")
            {
                //
                SplitStorageFiles.SplitStorageFile();

            }
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
        }


    }
}
