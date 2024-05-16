using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace ProvisionDeprovision
{
    class Program
    {
        private static string localConnectionString = "Server=localhost;Database=DigitalJudge_20231204_1032EST;Trusted_Connection=True;TrustServerCertificate=True";
        private static string cloudConnectionString = "Server=mocato-sqls-stride-dev.database.windows.net;Database=carlo-test-mocato-db-stride-dev_20240427_1013ET;User Id=servermin;";
        //private static string cloudConnectionString = "Server=localhost;Database=ServerB;Trusted_Connection=True;TrustServerCertificate=True";

        private static string serverConn = localConnectionString;
        private static string localConn = cloudConnectionString;

        private static string mode = "DC";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Begin Sync!");
            //await ProvisionServerManuallyAsync();
            //await ProvisionClientManuallyAsync();

            //await SynchronizeAsync();
            await Sync(mode);
            //await CreateSnapshot();

            //Console.WriteLine("Hello World!");
            //await SynchronizeAsyncProgress();
            //await DeprovisionServerManuallyAsync();
            //await DeprovisionClientManuallyAsync();

        }


        public static string GenerateId(int length)
        {
            // Create a new GUID instance
            Guid guid = Guid.NewGuid();

            // Convert the GUID to a base32 string (alphanumeric characters only)
            string randomAlphanumericId = guid.ToString("N");

            // Optionally, return a substring of the desired length
            if (length > 0 && length < randomAlphanumericId.Length)
            {
                return randomAlphanumericId.Substring(0, length);
            }

            return randomAlphanumericId;
        }

        private static async Task Sync(string mode) 
        {
            List<string> messages = new List<string>();
            List<string> tables = new List<string> {   "Person_T", "Position_T", "Organization_T", "Score_T", "Performance_Person_T", "Performance_T", };
            //List<string> tables = new List<string> {   "Person_T", "Position_T", "Organization_T",  "Performance_Person_T", "Performance_T", };       

            // Loop through the list
            foreach (var table in tables)
            {
                // Call the function that might throw an exception
                await SingleTableSync(table,mode);
            }
        }

        private static async Task SingleTableSync(string table, string mode)
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Create 2 Sql Sync providers
            // First provider is using the Sql change tracking feature. Don't forget to enable it on your database until running this code !
            // For instance, use this SQL statement on your server database : ALTER DATABASE AdventureWorks  SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)  
            // Otherwise, if you don't want to use Change Tracking feature, just change 'SqlSyncChangeTrackingProvider' to 'SqlSyncProvider'
            var serverProvider = new SqlSyncChangeTrackingProvider(serverConn);
            //var clientProvider = new SqlSyncProvider(clientConnectionString);
            var clientProvider = new SqlSyncChangeTrackingProvider(localConn);


            //var localOrchestrator = new LocalOrchestrator(serverProvider);
            //var remoteOrchestrator = new RemoteOrchestrator(clientProvider);



            // Tables involved in the sync process:
            //var setup = new SyncSetup("Score_T");
            //setup.Tables["Score_T"].SyncDirection = SyncDirection.DownloadOnly;


            // Tables involved in the sync process:
            var setup = new SyncSetup(table);

            setup.Tables[table].SyncDirection = SyncDirection.DownloadOnly;

            if (table == "Performance_Person_T")
            {
                setup.Tables["Performance_Person_T"].Columns.AddRange("Performance_ID", "Person_Position_Organization_ID",
                "Timestamp_DT", "Timestamp_TS", "PerformanceId", "PersonPositionOrganizationId",
                "Id", "TenantId", "ExtraProperties", "ConcurrencyStamp", "CreatorId", "CreationTime",
                "LastModifierId", "LastModificationTime", "EntityVersion");
            }

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            //var agent = new SyncAgent(localOrchestrator, remoteOrchestrator);

            agent.Options.BatchSize = 10;

            agent.Options.DisableConstraintsOnApplyChanges = true;

            agent.Options.ErrorResolutionPolicy = ErrorResolution.ContinueOnError;


            // Using the IProgress<T> pattern to handle progession dring the synchronization
            // Be careful, Progress<T> is not synchronous. Use SynchronousProgress<T> instead !
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

            // --------------------------------------------
            // Using Interceptors
            // --------------------------------------------

            // CancellationTokenSource is used to cancel a sync process in the next example
            var cts = new CancellationTokenSource();


            // Intercept a table changes selecting
            // Because the changes are not yet selected, we can easily interrupt the process with the cancellation token
            agent.LocalOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.WriteLine($"-------- Getting changes from table {args.SchemaTable.GetFullName()} ...");

                if (args.SchemaTable.TableName == "Table_That_Should_Not_Be_Sync")
                    cts.Cancel();
            });

            // Row has been selected from datasource.
            // You can change the synrow before the row is serialized on the disk.
            agent.LocalOrchestrator.OnRowsChangesSelected(args =>
            {
                Console.Write(".");
            });

            // Tables changes have been selected
            // we can have all the batch part infos generated
            agent.RemoteOrchestrator.OnTableChangesSelected(tcsa =>
            {
                Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
                    $"Files generated count:{tcsa.BatchPartInfos.Count()}. " +
                    $"Rows Count:{tcsa.TableChangesSelected.TotalChanges}");
            });

            // ErrorResolution.Throw is the default resolution. No need to explicitly set it.
            // It's done here for the demo explanation.
            agent.LocalOrchestrator.OnApplyChangesErrorOccured(args =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERROR: {args.Exception.Message}");
                Console.WriteLine($"ROW  : {args.ErrorRow}");
                Console.ResetColor();

                args.Resolution = ErrorResolution.ContinueOnError;
            });

            agent.RemoteOrchestrator.OnApplyChangesErrorOccured(args =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERROR: {args.Exception.Message}");
                Console.WriteLine($"ROW  : {args.ErrorRow}");
                Console.ResetColor();

                args.Resolution = ErrorResolution.ContinueOnError;
            });


            // This event is raised when a table is applying some rows, available on the disk
            agent.LocalOrchestrator.OnTableChangesApplying(args =>
            {
                Console.WriteLine($"Table {args.SchemaTable.GetFullName()}: " +
                    $"Applying changes from {args.BatchPartInfos.Count()} files. " +
                    $"{args.BatchPartInfos.Sum(bpi => bpi.RowsCount)} rows.");
            });

            // This event is raised for each batch rows (maybe 1 or more in each batch)
            // that will be applied on the datasource
            // You can change something to the rows before they are applied here
            agent.LocalOrchestrator.OnRowsChangesApplying(args =>
            {
                foreach (var syncRow in args.SyncRows)
                    Console.Write(".");
            });

            // This event is raised once all rows for a table have been applied
            agent.LocalOrchestrator.OnTableChangesApplied(args =>
            {
                Console.WriteLine();
                Console.WriteLine($"Table applied: ");
            });

            //do
            //{
            //    // Launch the sync process
            //    var s1 = await agent.SynchronizeAsync("Test 11", setup, SyncType.Normal, null, cts.Token, progress);
            //    // Write results
            //    Console.WriteLine(s1);

            //} while (Console.ReadKey().Key != ConsoleKey.Escape);

            try
            {
                
                var s1 = await agent.SynchronizeAsync($"{table}-{mode}-4", setup, SyncType.Normal, null, cts.Token, progress);
                Console.WriteLine(s1);
                //await SynchronizeAsyncProgress();
                //await DeprovisionServerManuallyAsync();
                //await DeprovisionClientManuallyAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine("End");
        }

        private static async Task CreateSnapshot() 
        {
            var serverProvider = new SqlSyncProvider(serverConn);

            // new setup with all tables involved
            var setup = new SyncSetup("Performance_T"
                 , "Score_T"
                 , "Performance_Person_T"
                 , "Person_T"
                 , "Position_T"
                 , "Organization_T");

            // snapshot directory
            var snapshotDirectoryName = "snapshots";
            var snapshotDirctory = Path.Combine(Environment.CurrentDirectory, "Snapshots");

            var options = new SyncOptions
            {
                SnapshotsDirectory = snapshotDirctory,
                BatchSize = 3000
            };

            // Create a remote orchestrator
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options);

            // Create a snapshot
            await remoteOrchestrator.CreateSnapshotAsync(setup);
        }

        private static async Task SynchronizeAsyncProgress()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Create 2 Sql Sync providers
            // First provider is using the Sql change tracking feature. Don't forget to enable it on your database until running this code !
            // For instance, use this SQL statement on your server database : ALTER DATABASE AdventureWorks  SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)  
            // Otherwise, if you don't want to use Change Tracking feature, just change 'SqlSyncChangeTrackingProvider' to 'SqlSyncProvider'
            var serverProvider = new SqlSyncChangeTrackingProvider(serverConn);
            //var clientProvider = new SqlSyncProvider(clientConnectionString);
            var clientProvider = new SqlSyncChangeTrackingProvider(localConn);

            // Tables involved in the sync process:
            //var setup = new SyncSetup("Score_T");
            //setup.Tables["Score_T"].SyncDirection = SyncDirection.DownloadOnly;


            var setup = new SyncSetup(
                 "Performance_T"
                 , "Score_T"
                 , "Performance_Person_T"
                 , "Person_T"
                 , "Position_T"
                 , "Organization_T"
                );

            setup.Tables["Performance_Person_T"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Score_T"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Performance_Person_T"].Columns.AddRange("Performance_ID", "Person_Position_Organization_ID",
    "Timestamp_DT", "Timestamp_TS", "PerformanceId", "PersonPositionOrganizationId",
    "Id", "TenantId", "ExtraProperties", "ConcurrencyStamp", "CreatorId", "CreationTime",
    "LastModifierId", "LastModificationTime", "EntityVersion");
            setup.Tables["Person_T"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Position_T"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Organization_T"].SyncDirection = SyncDirection.DownloadOnly;

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            agent.Options.BatchSize = 50;

            agent.Options.DisableConstraintsOnApplyChanges = true;

            agent.Options.SnapshotsDirectory = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                "Snapshots");

            // Using the IProgress<T> pattern to handle progession dring the synchronization
            // Be careful, Progress<T> is not synchronous. Use SynchronousProgress<T> instead !
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

            // --------------------------------------------
            // Using Interceptors
            // --------------------------------------------

            // CancellationTokenSource is used to cancel a sync process in the next example
            var cts = new CancellationTokenSource();


            // Intercept a table changes selecting
            // Because the changes are not yet selected, we can easily interrupt the process with the cancellation token
            agent.LocalOrchestrator.OnTableChangesSelecting(args =>
            {
                Console.WriteLine($"-------- Getting changes from table {args.SchemaTable.GetFullName()} ...");

                if (args.SchemaTable.TableName == "Table_That_Should_Not_Be_Sync")
                    cts.Cancel();
            });

            // Row has been selected from datasource.
            // You can change the synrow before the row is serialized on the disk.
            agent.LocalOrchestrator.OnRowsChangesSelected(args =>
            {
                Console.Write(".");
            });

            // Tables changes have been selected
            // we can have all the batch part infos generated
            agent.RemoteOrchestrator.OnTableChangesSelected(tcsa =>
            {
                Console.WriteLine($"Table {tcsa.SchemaTable.GetFullName()}: " +
                    $"Files generated count:{tcsa.BatchPartInfos.Count()}. " +
                    $"Rows Count:{tcsa.TableChangesSelected.TotalChanges}");
            });


            // This event is raised when a table is applying some rows, available on the disk
            agent.LocalOrchestrator.OnTableChangesApplying(args =>
            {
                Console.WriteLine($"Table {args.SchemaTable.GetFullName()}: " +
                    $"Applying changes from {args.BatchPartInfos.Count()} files. " +
                    $"{args.BatchPartInfos.Sum(bpi => bpi.RowsCount)} rows.");
            });

            // This event is raised for each batch rows (maybe 1 or more in each batch)
            // that will be applied on the datasource
            // You can change something to the rows before they are applied here
            agent.LocalOrchestrator.OnRowsChangesApplying(args =>
            {
                foreach (var syncRow in args.SyncRows)
                    Console.Write(".");
            });

            // This event is raised once all rows for a table have been applied
            agent.LocalOrchestrator.OnTableChangesApplied(args =>
            {
                Console.WriteLine();
                Console.WriteLine($"Table applied: ");
            });

            //do
            //{
            //    // Launch the sync process
            //    var s1 = await agent.SynchronizeAsync("Test 11", setup, SyncType.Normal, null, cts.Token, progress);
            //    // Write results
            //    Console.WriteLine(s1);

            //} while (Console.ReadKey().Key != ConsoleKey.Escape);

            try
            {
                var s1 = await agent.SynchronizeAsync("Default Scope", setup, SyncType.Normal, null, cts.Token, progress);
                Console.WriteLine(s1); ;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine("End");
        }


       


        


        private static async Task DeprovisionServerManuallyAsync()
        {
            // Create server provider
            var serverProvider = new SqlSyncProvider(serverConn);

            // Create a server orchestrator used to Deprovision everything on the server side
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            // Deprovision everything
            var p = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient |
                    SyncProvision.StoredProcedures | SyncProvision.TrackingTable |
                    SyncProvision.Triggers;

            // Deprovision everything
            await remoteOrchestrator.DeprovisionAsync(p);
        }
        private static async Task DeprovisionClientManuallyAsync()
        {
            // Create client provider
            var clientProvider = new SqlSyncProvider(localConn);

            // Create a local orchestrator used to Deprovision everything
            var localOrchestrator = new LocalOrchestrator(clientProvider);

            var p = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient |
                    SyncProvision.StoredProcedures | SyncProvision.TrackingTable |
                    SyncProvision.Triggers;

            // Deprovision everything
            await localOrchestrator.DeprovisionAsync(p);

        }


        
    }
}
