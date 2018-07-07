using System;
using QlikGoogleCloudConnector;

namespace QlikGoogleCloudConnector
{
    static class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            if (args != null && args.Length >= 2)
            {
                new QlikGoogleCloudConnectorServer().Run(args[0], args[1]);
                //var t = new QvEventLogServer().CreateConnectionString();
            }

            //new QvEventLogServer().CreateConnection();

            //new QvEventLogServer().Run("", "TEST");
        }
    }
}
