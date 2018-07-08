using System;
using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;   
using QlikView.Qvx.QvxLibrary;
//using Google.Cloud.Storage.V1;
//using Google.Apis.Auth.OAuth2;
//using Google.Apis.Storage.v1.Data;
//using Google.Apis.Storage.v1;
//using System.IO;
//using Google.Apis.Services;
//using System.Diagnostics;

namespace QlikGoogleCloudConnector
{
    internal class QlikGoogleCloudConnectorConnection : QvxConnection
    {
        string jsonPath = "";
        string jsonCredentials = "";
        string GCPProjectId = "";
        public override void Init()
        {
            QvxLog.SetLogLevels(true, true);

            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "Init()");
            
            this.MParameters.TryGetValue("jsonPath", out jsonPath);
            jsonCredentials = File.ReadAllText(jsonPath);



            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, jsonCredentials);

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

            var dummyFields = new QvxField[]
            {
                    new QvxField("DummyField", QvxFieldType.QVX_TEXT, QvxNullRepresentation.QVX_NULL_FLAG_SUPPRESS_DATA, FieldAttrType.ASCII)
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
                            //GetRows = GetBucketObjects,
                            Fields = objectFields
                        },
                    new QvxTable
                        {
                            TableName = "DownloadObject",
                            Fields = dummyFields
                        },
                    new QvxTable
                        {
                            TableName = "UploadObject",
                            Fields = dummyFields
                        },
                    new QvxTable
                        {
                            TableName = "DeleteLocalObject",
                            Fields = dummyFields
                        }
                };
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

                //QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, b);

                var c = b.Split(new[] { "and" }, StringSplitOptions.None);

                for (int i = 0; i < c.Length; i++)
                {
                    var f = c[i].Trim();
                    var g = f.Split('=');
                    
                    string value = g[1];
                    value = value.Replace("=", "").Trim();
                    value = value.Replace("'", "");

                    dict.Add(g[0].Trim(), value.Trim());
                }

            }
            catch (Exception ex)
            {
                throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, String.Format("Error parsing the WHERE clause. WHERE clause is present?"));
            }

            if (dict.Count == 0)
            {
                throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, "Please provide WHERE clause");
            }

                return dict;
        }

        public override QvxDataTable ExtractQuery(string line, List<QvxTable> qvxTables)
        {
            string tableName = GetTableName(line);
            IDictionary<string, string> fields = GetWhereFields(line);
            QvxDataTable returnTable = null;

            //foreach (KeyValuePair<string, string> field in fields)
            //{
            //    QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, " ------>>>>>>>>>>> " + field.Key + "   " + field.Value );
            //}

            switch (tableName)
            {
                case "listbuckets":
                    QvxDataTable a = StorageOperations.ListBuckets(FindTable("ListBuckets", MTables), fields, jsonCredentials);
                    returnTable = a;
                    break;
                case "bucketobjects":
                    QvxDataTable a1 = StorageOperations.ListBucketObjects(FindTable("BucketObjects", MTables), fields, jsonCredentials);
                    returnTable = a1;
                    break;
                case "downloadobject":
                    QvxDataTable downloadObj = StorageOperations.DownloadObject(FindTable("DownloadObject", MTables), fields, jsonCredentials);
                        //ListBucketObjects(FindTable("BucketObjects", MTables), fields, jsonCredentials);
                    returnTable = downloadObj;
                    //DownloadObject("countnazgul-test", "dbip-isp-000.csv", @"c:\GCP_TEST\dbip-isp-000.csv");
                    //DownloadObject("countnazgul-test", "code/WebService.zip", @"c:\GCP_TEST\WebService.zip");
                    break;
                case "uploadobject":
                    // TBA
                    break;
                case "deletelocalobject":
                    // TBA
                    break;
                default:
                    throw new QvxPleaseSendReplyException(QvxResult.QVX_UNKNOWN_COMMAND, "Please provide WHERE clause");
                    
            }

            return returnTable;
        }
    }
}
