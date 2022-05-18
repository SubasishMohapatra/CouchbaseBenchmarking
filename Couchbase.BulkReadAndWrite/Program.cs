using Couchbase.Extensions.Caching;
using Couchbase.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using System;
using System.Threading.Tasks;

namespace Couchbase.BulkReadAndWrite
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //Console.WriteLine("Hello");
            //Console.ReadLine();
            var config = new ConfigurationBuilder()
    .AddJsonFile("hosting.json", optional: true) //this is not needed, but could be useful
    .AddCommandLine(args)
    .Build();
            using IHost host = CreateHostBuilder(args).Build();
            await host.RunAsync();
            Console.ReadLine();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
              .ConfigureAppConfiguration((hostingContext, config) =>
              {
                  config.AddCommandLine(args);
              })
               .ConfigureServices((_, services) =>
               {
                   var serilogLogger = new LoggerConfiguration()
              .WriteTo.Console()
              .WriteTo.RollingFile("log.txt", shared: true)
              .CreateLogger();
                   services.AddLogging(builder =>
                   {
                       builder.SetMinimumLevel(LogLevel.Warning);
                       builder.AddSerilog(logger: serilogLogger, dispose: true);
                   });
                   services.AddCouchbase(options =>
                   options
                   .WithConnectionString("couchbase://localhost")
                   //.WithConnectionString("couchbase://server1:11210,server2:11210,server3:11210")
                   //.WithConnectionString("couchbase://server1,server2,server3")
                   //.WithConnectionString("couchbase://cb-cluster")
                   .WithCredentials("Administrator", "*******")
                   .WithBuckets("Cache-Sample"));
                   services.AddCouchbaseBucket<ICouchbaseCacheBucketProvider>("Cache-Sample");
                   services.AddDistributedCouchbaseCache("Cache-Sample", opt => { });
                   services.AddHostedService<BulkOperationsService>();
                   services.AddHostedService<PlanScenarioBackgroundWorker>();
               });
    }
}
