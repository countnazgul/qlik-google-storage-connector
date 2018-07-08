using Google.Apis.Auth.OAuth2;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using QlikView.Qvx.QvxLibrary;
using System;
using System.Collections.Generic;
using System.IO;
using System.Web.Script.Serialization;

namespace QlikGoogleCloudConnector
{
    class StorageOperations
    {
        public static QvxDataTable ListBuckets(QvxTable bucketsTable, IDictionary<string, string> fields, string jsonCredentials)
        {
            bucketsTable.GetRows = ListBucketsRows(bucketsTable, fields, jsonCredentials);

            return new QvxDataTable(bucketsTable);
        }

        public static QvxTable.GetRowsHandler ListBucketsRows(QvxTable table, IDictionary<string, string> fields, string jsonCredentials)
        {
            var JSONObj = new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(jsonCredentials);
            string GCPProjectId = JSONObj["project_id"];

            return () =>
            {
                List<QvxDataRow> rows = new List<QvxDataRow>();

                var credential = GoogleCredential.FromJson(jsonCredentials);
                var storage = StorageClient.Create(credential);

                var buckets = storage.ListBuckets(GCPProjectId);

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



        public static QvxDataTable ListBucketObjects(QvxTable bucketObjectsTable, IDictionary<string, string> fields, string jsonCredentials)
        {
            bucketObjectsTable.GetRows = ListBucketObjectsRows(fields, bucketObjectsTable, jsonCredentials);

            return new QvxDataTable(bucketObjectsTable);
        }

        public static QvxTable.GetRowsHandler ListBucketObjectsRows(IDictionary<string, string> fields, QvxTable table, string jsonCredentials)
        {
            return () =>
            {
                List<QvxDataRow> rows = new List<QvxDataRow>();

                //var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
                var credential = GoogleCredential.FromJson(jsonCredentials);
                var storage = StorageClient.Create(credential);

                fields.TryGetValue("bucketname", out string bucketName);

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



        public static QvxDataTable DownloadObject(QvxTable downloadObjectTable, IDictionary<string, string> fields, string jsonCredentials)
        {
            var credential = GoogleCredential.FromJson(jsonCredentials);
            var storage = StorageClient.Create(credential);

            fields.TryGetValue("bucketname", out string bucketName);
            fields.TryGetValue("objectname", out string objectName);
            fields.TryGetValue("localpath", out string localPath);

            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice,  String.Format(" ------>>>>>>>>>>> {0}, {1}, {2}", bucketName, objectName, localPath));
            localPath = localPath ?? Path.GetFileName(objectName);
            using (var outputFile = File.OpenWrite(localPath))
            {
                storage.DownloadObject(bucketName, objectName, outputFile);
            }
            downloadObjectTable.GetRows = DownloadObjectRows(downloadObjectTable);
            return new QvxDataTable(downloadObjectTable);

        }

        public static QvxTable.GetRowsHandler DownloadObjectRows(QvxTable table)
        {
            return () =>
            {
                List<QvxDataRow> rows = new List<QvxDataRow>();

                var row = new QvxDataRow();
                row[table.Fields[0]] = "Dummy Value. This table can be dropped from the data model.";
                rows.Add(row);
                
                return rows;
            };
        }
    }
}
