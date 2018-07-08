using QlikView.Qvx.QvxLibrary;
using System;
using System.Windows.Interop;

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
            var connectionConfig = CreateLoginWindowHelper();
            connectionConfig.ShowDialog();

            string connectionString = null;
            if (connectionConfig.DialogResult.Equals(true))
            {
                connectionString = String.Format("jsonPath={0}", connectionConfig.GetJSONPath());
            }

            return connectionString;
        }

        private ConnectionConfig CreateLoginWindowHelper()
        {
            var config = new ConnectionConfig();
            var wih = new WindowInteropHelper(config);
            wih.Owner = MParentWindow;

            return config;
        }
    }
}

