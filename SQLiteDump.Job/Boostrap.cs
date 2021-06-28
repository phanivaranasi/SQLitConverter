using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Events;
using SQLiteDump.Job.SQLiteBuilder;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteDump.Job
{
    public class Boostrap
    {
        private readonly IConfigurationRoot configuration;
        private readonly IServiceProvider provider;
        private readonly ILogger logger;

        public IServiceProvider Provider => provider;
        public ILogger Logger => logger;

        public Boostrap()
        {
            string environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            configuration = new ConfigurationBuilder()
                        .SetBasePath(Directory.GetCurrentDirectory())
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .AddJsonFile($"appsettings.{environment}.json", optional: true)
                        .AddEnvironmentVariables()
                        .Build();


            logger = new LoggerConfiguration()
                    .WriteTo.File(@"apploog.log", rollingInterval: RollingInterval.Hour)
                    .CreateLogger();

            ServiceCollection services = new ServiceCollection();

            services.AddLogging(configure => configure.AddSerilog())
                .AddSingleton<IConfiguration>(configuration)
                .AddSingleton<ILogger>(logger)
                .AddTransient<IFileBuilder, FileBuilder>()
                .AddSingleton<ISQLiteConverter, SQLiteConverter>();



            provider = services.BuildServiceProvider();

        }
    }
}
