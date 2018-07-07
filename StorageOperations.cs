using Google.Apis.Auth.OAuth2;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using QlikView.Qvx.QvxLibrary;
using System;
using System.Collections.Generic;
using System.IO;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

namespace QlikGoogleCloudConnector
{
    class StorageOperations
    {
        public static string Test()
        {
            return "123";
        }

        public static QvxDataTable ListBuckets(string projectId, QvxTable bucketsTable)
        {
            bucketsTable.GetRows = ListBucketsRows(projectId, bucketsTable);

            return new QvxDataTable(bucketsTable);
        }

        public static QvxTable.GetRowsHandler ListBucketsRows(string projectId, QvxTable table)
        {
            return () =>
            {
                List<QvxDataRow> rows = new List<QvxDataRow>();

                var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
                var storage = StorageClient.Create(credential);
                var buckets = storage.ListBuckets(projectId);

                foreach (Bucket bucket in buckets)
                {
                    var row = new QvxDataRow();
                    row[table.Fields[0]] = bucket.Acl?.ToString() ?? "";
                    row[table.Fields[1]] = bucket.Billing?.ToString() ?? "";
                    row[table.Fields[2]] = bucket.Cors?.ToString() ?? "";
                    row[table.Fields[3]] = bucket.ETag?.ToString() ?? "";
                    row[table.Fields[4]] = bucket.Encryption?.ToString() ?? "";
                    row[table.Fields[5]] = bucket.Id?.ToString() ?? "";
                    row[table.Fields[6]] = bucket.Kind?.ToString() ?? "";
                    row[table.Fields[7]] = bucket.Labels?.ToString() ?? "";
                    row[table.Fields[8]] = bucket.Lifecycle?.ToString() ?? "";
                    row[table.Fields[9]] = bucket.Location?.ToString() ?? "";
                    row[table.Fields[10]] = bucket.Logging?.ToString() ?? "";
                    row[table.Fields[11]] = bucket.Name?.ToString() ?? "";
                    row[table.Fields[12]] = bucket.Owner?.ToString() ?? "";
                    rows.Add(row);
                }



                return rows;
            };
        }

        public static QvxDataTable ListBucketObjects(string bucketName, QvxTable bucketObjectsTable)
        {
            bucketObjectsTable.GetRows = ListBucketObjectsRows(bucketName, bucketObjectsTable);

            return new QvxDataTable(bucketObjectsTable);
        }

        public static QvxTable.GetRowsHandler ListBucketObjectsRows(string bucketName, QvxTable table)
        {
            return () =>
            {
                List<QvxDataRow> rows = new List<QvxDataRow>();

                var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
                var storage = StorageClient.Create(credential);
                var bucketObjects = storage.ListObjects(bucketName, null, null);


                foreach (Google.Apis.Storage.v1.Data.Object bucketObject in bucketObjects)
                {
                    var row = new QvxDataRow();
                    row[table.Fields[0]] = bucketObject.Acl?.ToString() ?? "";
                    row[table.Fields[1]] = bucketObject.Bucket?.ToString() ?? "";
                    row[table.Fields[2]] = bucketObject.CacheControl?.ToString() ?? "";
                    row[table.Fields[3]] = bucketObject.ComponentCount ?? 0;
                    row[table.Fields[4]] = bucketObject.ContentDisposition?.ToString() ?? "";
                    row[table.Fields[5]] = bucketObject.ContentEncoding?.ToString() ?? "";
                    row[table.Fields[6]] = bucketObject.ContentLanguage?.ToString() ?? "";
                    row[table.Fields[7]] = bucketObject.Crc32c.ToString() ?? "";
                    row[table.Fields[8]] = bucketObject.ETag?.ToString() ?? "";
                    row[table.Fields[9]] = bucketObject.Generation?.ToString() ?? "";
                    row[table.Fields[10]] = bucketObject.Id?.ToString() ?? "";
                    row[table.Fields[11]] = bucketObject.Kind?.ToString() ?? "";
                    row[table.Fields[12]] = bucketObject.KmsKeyName?.ToString() ?? "";
                    row[table.Fields[13]] = bucketObject.Md5Hash?.ToString() ?? "";
                    row[table.Fields[14]] = bucketObject.MediaLink?.ToString() ?? "";
                    row[table.Fields[15]] = ""; // bucketObject.Metadata?.ToString() ?? "";
                    row[table.Fields[16]] = bucketObject.Metageneration ?? 0;
                    row[table.Fields[17]] = bucketObject.Name?.ToString() ?? "";
                    row[table.Fields[18]] = bucketObject.Owner?.ToString() ?? "";
                    row[table.Fields[19]] = bucketObject.SelfLink?.ToString() ?? "";
                    row[table.Fields[20]] = bucketObject.Size ?? 0;
                    row[table.Fields[21]] = bucketObject.StorageClass?.ToString() ?? "";
                    row[table.Fields[22]] = bucketObject.TimeCreatedRaw?.ToString() ?? "";
                    row[table.Fields[23]] = bucketObject.TimeDeletedRaw?.ToString() ?? "";
                    row[table.Fields[24]] = bucketObject.TimeStorageClassUpdatedRaw?.ToString() ?? "";
                    row[table.Fields[25]] = bucketObject.UpdatedRaw?.ToString() ?? "";                    
                    rows.Add(row);
                }



                return rows;
            };
        }

        private void DownloadObject(string bucketName, string objectName, string localPath = null)
        {

            var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
            var storage = StorageClient.Create(credential);
            //var buckets = storage.ListBuckets("local-shoreline-645");

            //var storage = StorageClient.Create();
            localPath = localPath ?? Path.GetFileName(objectName);
            using (var outputFile = File.OpenWrite(localPath))
            {
                storage.DownloadObject(bucketName, objectName, outputFile);
            }
            Console.WriteLine($"downloaded {objectName} to {localPath}.");
        }
    }
}
