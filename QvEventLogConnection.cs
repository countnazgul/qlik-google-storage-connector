using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.RegularExpressions;   
using QlikView.Qvx.QvxLibrary;
using Google.Cloud.Storage.V1;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Storage.v1.Data;
using Google.Apis.Storage.v1;
using System.IO;
using Google.Apis.Services;

namespace QvEventLogConnectorSimple
{
    internal class QvEventLogConnection : QvxConnection
    {
        public override void Init()
        {
            QvxLog.SetLogLevels(true, true);

            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init()");

            var bucketsListFields = new QvxField[]
                {
                    new QvxField("Acl", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Billing", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Cors", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("ETag", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Encryption", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Id", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Kind", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Label", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Lifecycle", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Location", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Logging", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Name", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Owner", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII)
                };

            var objectFields = new QvxField[]
                {
                    new QvxField("Acl", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Bucket", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("CacheControl", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("ComponentCount", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.INTEGER),
                    new QvxField("ContentDisposition", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("ContentEncoding", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("ContentLanguage", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Crc32c", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("ETag", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Generation", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Id", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Kind", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("KmsKeyName", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Md5Hash", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("MediaLink", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Metadata", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Metageneration", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.INTEGER),
                    new QvxField("Name", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Owner", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("SelfLink", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Size", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.INTEGER),
                    new QvxField("StorageClass", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("TimeCreated", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),                    
                    new QvxField("TimeDeleted", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("TimeStorageClassUpdated", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    new QvxField("Updated", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII)

                    //new QvxField("CustomerEncryption", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    //new QvxField("TimeCreatedRaw", QvxFieldType.QVX_SIGNED_INTEGER, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    //new QvxField("TimeDeletedRaw", QvxFieldType.QVX_SIGNED_INTEGER, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    //new QvxField("TimeStorageClassUpdated", QvxFieldType.QVX_SIGNED_INTEGER, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                    //new QvxField("Updated", QvxFieldType.QVX_SIGNED_INTEGER, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
                };

            MTables = new List<QvxTable>
                {
                    new QvxTable
                        {
                            TableName = "ListBuckets",
                            //GetRows = GetBucketsList,                            
                            Fields = bucketsListFields
                        },
                    new QvxTable
                        {
                            TableName = "BucketObjects",
                            GetRows = GetBucketObjects,
                            Fields = objectFields
                        }
                };
        }

        public IEnumerable<QvxDataRow> GetBucketsList()
        {
            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "GetApplicationEvents()");

            var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
            var storage = StorageClient.Create(credential);
            var buckets = storage.ListBuckets("local-shoreline-645");            

            foreach (Bucket b in buckets)
            {
                yield return MakeEntry(b, FindTable("Buckets", MTables));
            }
        }

        public QvxDataRow MakeEntry(Bucket bucket, QvxTable table)
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
            return row;
        }


        public IEnumerable<QvxDataRow> GetBucketObjects()
        {
            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "GetApplicationEvents()");

            var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
            var storage = StorageClient.Create(credential);
            var bucketObjects = storage.ListObjects("countnazgul-test", null, null);

            foreach (Google.Apis.Storage.v1.Data.Object o in bucketObjects)
            {
                yield return MakeEntryObjects(o, FindTable("BucketObjects", MTables));
            }
        }

        public QvxDataRow MakeEntryObjects(Google.Apis.Storage.v1.Data.Object bucketObject, QvxTable table)
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
            return row;
        }


        public IEnumerable<QvxDataRow> GetBucketsList1(string test, QvxTable tbl)
        {
            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "GetApplicationEvents()");

            var credential = GoogleCredential.FromFile(@"c:\Users\Administrator\Desktop\local-shoreline-645-b36405e68311.json");
            var storage = StorageClient.Create(credential);
            var buckets = storage.ListBuckets("local-shoreline-645");

            foreach (Bucket b in buckets)
            {
                yield return MakeEntry1(b,tbl);
            }
        }

        public QvxDataRow MakeEntry1(Bucket bucket, QvxTable table)
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
            return row;
        }


        private QvxTable.GetRowsHandler mapRows(string app, QvxTable table)
        {
            return () =>
            {
                // Implement context based stuffz
                return new List<QvxDataRow>();
            };
        }


        private QvxTable.GetRowsHandler errorRows(string errorMessage, QvxTable table)
        {
            return () =>
            {
                // Implement context based stuffz
                List<QvxDataRow> rows = new List<QvxDataRow>();
                var row = new QvxDataRow();
                row[table.Fields[0]] = errorMessage;
                rows.Add(row);

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

        private string GetTableName(string query)
        {
            string tableName = "";

            var r = new Regex(@"(from|join)\s+(?<table>\S+)", RegexOptions.IgnoreCase);

            Match m = r.Match(query);
            while (m.Success)
            {
                tableName = m.Groups["table"].Value;
                m = m.NextMatch();
            }

            if( tableName.Length == 0)
            {
                throw new QvxPleaseSendReplyException(QvxResult.QVX_TABLE_NOT_FOUND, "Table name is rquired");
            }


            QvxTable a = FindTable(tableName, MTables);
            if(a == null)
            {
                throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, String.Format("Table '{0}' not found", tableName));
            }

            return tableName.ToLower();
        }

        private IDictionary<string, string> GetWhereFields(string query)
        {
            IDictionary<string, string> dict = new Dictionary<string, string>();

            try
            {
                var b = query.Substring(query.ToLower().IndexOf("where"));
                b = b.ToLower();
                b = b.Replace("where", "");

                var c = b.Split(new[] { "and" }, StringSplitOptions.None);

                for (int i = 0; i < c.Length; i++)
                {
                    var f = c[i].Trim();
                    var g = f.Split('=');

                    dict.Add(g[0].Trim(), g[1].Replace("=", "").Trim());
                }

            }
            catch (Exception ex)
            {
                throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, ex.Message);
            }

            if (dict.Count == 0)
            {
                throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, "Please provide WHERE clause");
            }

                return dict;
        }

        public override QvxDataTable ExtractQuery(string line, List<QvxTable> qvxTables)
        {

            //DownloadObject("countnazgul-test", "dbip-isp-000.csv", @"c:\GCP_TEST\dbip-isp-000.csv");
            //DownloadObject("countnazgul-test", "code/WebService.zip", @"c:\GCP_TEST\WebService.zip");

            string tableName = GetTableName(line);
            //IDictionary<string, string> dict = GetWhereFields(line);
            QvxDataTable returnTable = null;
            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, tableName);
            switch (tableName)
            {
                case "listbuckets":
                    QvxDataTable a = StorageOperations.ListBuckets("local-shoreline-645", FindTable("ListBuckets", MTables));
                    returnTable = a;
                    break;
                case "bucketobjects":
                    QvxDataTable a1 = StorageOperations.ListBucketObjects("countnazgul-test", FindTable("BucketObjects", MTables));
                    returnTable = a1;
                    break;
                default:
                    throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, "Please provide WHERE clause");
                    break;
                    
            }

            return returnTable;
            //if (dict.Count == 0)
            //{
            //    //QvxReply t = new QvxReply();
            //    //t.ErrorMessage = "aaaaaaaaaaaaaaaaaa";
            //    throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, "Please provide WHERE clause");
            //    //throw new Exception("Please provide WHERE clause");

            //    var errorFields = new QvxField[]
            //        {
            //        new QvxField("Message", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII)
            //        };

            //    QvxTable errorTable = new QvxTable
            //    {
            //        TableName = "Error",
            //        Fields = errorFields
            //    };

            //    errorTable.GetRows = errorRows("Please provide WHERE clause", errorTable);

            //    //MTables.Add(errorTable);

            //    return new QvxDataTable(errorTable);
            //} else
            //{
            //    return new QvxDataTable(qvxTables[1]);
            //}

            //switch (tableName)
            //{
            //    case "Buckets":

            //        break;
            //}



            //var bucketsListFields = new QvxField[]
            //    {
            //        new QvxField("Acl", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Billing", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Cors", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("ETag", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Encryption", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Id", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Kind", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Label", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Lifecycle", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Location", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Logging", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Name", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII),
            //        new QvxField("Owner", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII)
            //    };

            //QvxTable table = new QvxTable
            //{
            //    TableName = "NewTable",
            //    Fields = bucketsListFields
            //};

            //table.GetRows = mapRows("TEST", table);

            //MTables.Add(table);

            //QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, qvxTables[1].TableName.ToString());
            //query = Regex.Replace(query, "\\\"", "'");

            //return new QvxDataTable(qvxTables[1]);


            //return null;
            //return base.ExtractQuery(query, qvxTables);
            //return new QvxDataTable(new QvxTable() { TableName = "Error" });
        }
    }
}
