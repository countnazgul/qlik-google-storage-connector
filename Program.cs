using System;
//using QlikGoogleCloudConnector;
using System.Windows.Forms;
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
            }
            else if (args != null && args.Length == 0)
            {
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new Standalone());
            }

            //new QvEventLogServer().CreateConnection();
        }
    }
}
