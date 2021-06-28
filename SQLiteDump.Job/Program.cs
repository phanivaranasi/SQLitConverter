using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace SQLiteDump.Job
{
    class Program
    {
        static void Main(string[] args)
        {
            Boostrap startup = new Boostrap();
            ISQLiteConverter service = startup.Provider.GetRequiredService<ISQLiteConverter>();
            service.IntiConversion();


            //Console.ReadLine();return;
        }
    }
}
