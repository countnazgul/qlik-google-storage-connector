using QlikView.Qvx.QvxLibrary;
using System;
using System.Windows.Interop;
using System.IO;
using System.Collections.Generic;

namespace QlikGoogleCloudConnector
{
    internal class QlikGoogleCloudConnectorServer : QvxServer
    {
        public override QvxConnection CreateConnection()
        {

            return new QlikGoogleCloudConnectorConnection();
        }

        public override string CreateConnectionString()
        {
            //var connectionConfig = CreateLoginWindowHelper();
            //connectionConfig.ShowDialog();

            //string connectionString = null;
            //if (connectionConfig.DialogResult.Equals(true))
            //{
            //    connectionString = String.Format("jsonPath={0}", connectionConfig.GetJSONPath());
            //}

            //return connectionString;

            QvxLog.Log(QvxLogFacility.Application, QvxLogSeverity.Notice, "CreateConnectionString()");
            return String.Format("jsonPath={0}", @"c:\Users\countnazgul\Downloads\qlik-cloud-storage-connector-79943f478bca.json");
        }

        public override string HandleJsonRequest(string method, string[] userParameters, QvxConnection connection)
        {
            QvDataContractResponse response;

            /**
             * -- How to get hold of connection details? --
             *
             * Provider, username and password are always available in
             * connection.MParameters if they exist in the connection
             * stored in the QlikView Repository Service (QRS).
             *
             * If there are any other user/connector defined parameters in the
             * connection string they can be retrieved in the same way as seen
             * below
             */

            string provider, host, username, password;
            connection.MParameters.TryGetValue("provider", out provider); // Set to the name of the connector by QlikView Engine
            connection.MParameters.TryGetValue("userid", out username); // Set when creating new connection or from inside the QlikView Management Console (QMC)
            connection.MParameters.TryGetValue("password", out password); // Same as for username
            connection.MParameters.TryGetValue("host", out host); // Defined when calling createNewConnection in connectdialog.js

            switch (method)
            {
                case "getInfo":
                    response = getInfo();
                    break;
                case "getDatabases":
                    response = getDatabases(username, password);
                    break;
                case "getTables":
                    response = getTables(username, password, connection, userParameters[0], userParameters[1]);
                    break;
                case "getFields":
                    response = getFields(connection, userParameters[0]);
                    break;
                case "testConnection":
                    response = testConnection(userParameters[0]);
                    break;
                default:
                    response = new Info { qMessage = "Unknown command" };
                    break;
            }
            return ToJson(response);    // serializes response into JSON string


        }

        private ConnectionConfig CreateLoginWindowHelper()
        {
            var config = new ConnectionConfig();
            var wih = new WindowInteropHelper(config);
            wih.Owner = MParentWindow;

            return config;
        }

        public QvDataContractResponse getInfo()
        {
            return new Info
            {
                qMessage = "Connector for loading data and meta data from Google Cloud Storage buckets."
            };
        }

        public QvDataContractResponse getDatabases(string username, string password)
        {

            var db = new Database[]
            { 
                new Database {
                    qName = "No database needed" 
                }
            };

            return new QvDataContractDatabaseListResponse
            {
                qDatabases = db
            };
        }

        public QvDataContractResponse getTables(string username, string password, QvxConnection connection, string database, string owner)
        {
            return new QvDataContractTableListResponse
            {
                qTables = connection.MTables
            };
        }

        public QvDataContractResponse getFields(QvxConnection connection, string table)
        {
            var currentTable = connection.FindTable(table, connection.MTables);

            return new QvDataContractFieldListResponse
                {
                    qFields = (currentTable != null) ? currentTable.Fields : new QvxField[0]
                };
        }

        public QvDataContractResponse testConnection(string jsonPath)
        {
            var message = jsonPath;
            string jsonCredentials = "";

            try
            {
                jsonCredentials = File.ReadAllText(jsonPath);
                message = "JSON file read successfully";
            }
            catch (Exception ex)
            {
                message = "File not found!";
            }

            return new Info { qMessage = message };
        }
    }
}

