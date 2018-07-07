using System;
using QvEventLogConnectorSimple;

namespace QvEventLogConnectorElaborate
{
    static class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            if (args != null && args.Length >= 2)
            {
                new QvEventLogServer().Run(args[0], args[1]);
                //var t = new QvEventLogServer().CreateConnectionString();
            }

            //new QvEventLogServer().CreateConnection();

            //new QvEventLogServer().Run("", "TEST");
        }
    }
}
