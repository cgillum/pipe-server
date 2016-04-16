namespace PipeServer
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Defines static properties for accessing well-known performance counter objects.
    /// </summary>
    public static class PipeServerPerformanceCounters
    {
        const string DefaultCategoryName = "Pipe Server";

        public static bool IsInitialized { get; private set; }

        /// <summary>
        /// The number of pipe server connections in active use at a given time.
        /// </summary>
        public static PerformanceCounter ActiveConnections { get; private set; }

        /// <summary>
        /// The number of pipe server connections established at any given time.
        /// </summary>
        public static PerformanceCounter CurrentConnections { get; private set; }

        /// <summary>
        /// The total number of failed pipe server connections over time.
        /// </summary>
        public static PerformanceCounter FailedConnections { get; private set; }

        /// <summary>
        /// The average time in milliseconds that it takes to establish a new connection.
        /// </summary>
        public static PerformanceCounter AverageConnectionWaitTime { get; private set; }

        /// <summary>
        /// Average base counter for <see cref="AverageConnectionWaitTime"/>.
        /// </summary>
        public static PerformanceCounter AverageConnectionWaitTimeBase { get; private set; }

        /// <summary>
        /// Installs the performance counters onto the local machine. This requires Administrative permissions.
        /// </summary>
        public static void Install(string categoryName = DefaultCategoryName)
        {
            CounterCreationDataCollection definitions = GetDefinitions();

            if (PerformanceCounterCategory.Exists(categoryName))
            {
                PerformanceCounterCategory.Delete(categoryName);
            }

            PerformanceCounterCategory.Create(
                categoryName: categoryName,
                categoryHelp: "Pipe Server performance counters",
                categoryType: PerformanceCounterCategoryType.MultiInstance,
                counterData: definitions);

            Initialize(categoryName);
        }

        public static void Initialize(string categoryName = DefaultCategoryName)
        {
            if (IsInitialized)
            {
                return;
            }

            string instanceName = GetInstanceName();

            CounterCreationDataCollection definitions = GetDefinitions();

            // IMPORTANT: Initialization order must match the creation order in GetDefinitions()!
            int index = 0;
            ActiveConnections = new PerformanceCounter(categoryName, definitions[index++].CounterName, instanceName, readOnly: false);
            ActiveConnections.RawValue = 0;

            CurrentConnections = new PerformanceCounter(categoryName, definitions[index++].CounterName, instanceName, readOnly: false);
            CurrentConnections.RawValue = 0;

            FailedConnections = new PerformanceCounter(categoryName, definitions[index++].CounterName, instanceName, readOnly: false);
            FailedConnections.RawValue = 0;

            AverageConnectionWaitTime = new PerformanceCounter(categoryName, definitions[index++].CounterName, instanceName, readOnly: false);
            AverageConnectionWaitTime.RawValue = 0;

            AverageConnectionWaitTimeBase = new PerformanceCounter(categoryName, definitions[index++].CounterName, instanceName, readOnly: false);
            AverageConnectionWaitTimeBase.RawValue = 0;

            IsInitialized = true;
        }

        static string GetInstanceName()
        {
            // IIS: Get the name of the application pool
            string[] commandLineArgs = Environment.GetCommandLineArgs();
            if (commandLineArgs[0].EndsWith("w3wp.exe", StringComparison.OrdinalIgnoreCase))
            {
                for (int i = 0; i < commandLineArgs.Length - 1; i++)
                {
                    if (commandLineArgs[i].Equals("-ap", StringComparison.OrdinalIgnoreCase))
                    {
                        return commandLineArgs[i + 1];
                    }
                }
            }

            // Not IIS: Get the name of the process
            using (Process currentProcess = Process.GetCurrentProcess())
            {
                return currentProcess.ProcessName;
            }
        }

        static CounterCreationDataCollection GetDefinitions()
        {
            return new CounterCreationDataCollection
            {
                new CounterCreationData(
                    counterName: "Active Connections",
                    counterHelp: "The number of pipe server connections in active use at a given time.",
                    counterType: PerformanceCounterType.NumberOfItems32),

                new CounterCreationData(
                    counterName: "Current Connections",
                    counterHelp: "The number of pipe server connections established at any given time.",
                    counterType: PerformanceCounterType.NumberOfItems32),

                new CounterCreationData(
                    counterName: "Failed Connections",
                    counterHelp: "The total number of failed pipe server connections over time.",
                    counterType: PerformanceCounterType.NumberOfItems32),

                // NOTE: For perforance counters which compute averages, the order in which the counters are created is important.
                //       Always add the base counter after the average counter.
                new CounterCreationData(
                    counterName: "Average Connection Wait Time",
                    counterHelp: "The number of seconds on average it takes to establish a new connection.",
                    counterType: PerformanceCounterType.AverageTimer32),
                new CounterCreationData(
                    counterName: "Average Connection Wait Time Base",
                    counterHelp: "Average base counter for Average Connection Wait Time",
                    counterType: PerformanceCounterType.AverageBase),
            };
        }
    }
}
