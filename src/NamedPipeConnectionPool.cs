namespace PipeServer
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO.Pipes;
    using System.Linq;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// Represents a pool of reusable connections to a single named-pipe endpoint.
    /// </summary>
    /// <remarks>
    /// The connection pool starts out with a number of connections equal to 4X the number of
    /// logical cores on the machine. Sufficient contention for connections will result in more
    /// connections being added until the connection pool size reaches 64X the number of cores.
    /// </remarks>
    public class NamedPipeConnectionPool : IDisposable
    {
        const int ConnectTimeoutMs = 5000;

        static readonly TimeSpan AddConnectionTimeout = TimeSpan.FromMilliseconds(100);
        static readonly TimeSpan WaitForConnectionTimeout = TimeSpan.FromSeconds(30);
        static readonly ConcurrentDictionary<string, NamedPipeConnectionPool> ConnectionPools =
            new ConcurrentDictionary<string, NamedPipeConnectionPool>(StringComparer.OrdinalIgnoreCase);

        readonly object createNewConnectionsLock = new object();

        static int currentMinPoolSize = (Environment.ProcessorCount * 64) - 1;
        static int currentMaxPoolSize = Environment.ProcessorCount * 64;

        string handshakePipeName;
        int minPoolSize;
        int maxPoolSize;
        BlockingCollection<NamedPipeClientStream> availablePool;
        IDictionary<NamedPipeClientStream, object> allConnections;
        bool isConnectionPoolInitialized;
        bool isDisposed;

        NamedPipeConnectionPool(string pipeName)
        {
            this.handshakePipeName = pipeName;
            this.minPoolSize = currentMinPoolSize;
            this.maxPoolSize = currentMaxPoolSize;
        }

        ~NamedPipeConnectionPool()
        {
            this.Dispose();
        }

        public static int MinPoolSize
        {
            get { return currentMinPoolSize; }
        }

        public static int MaxPoolSize
        {
            get { return currentMaxPoolSize; }
        }

        public static void SetPoolSize(int minPoolSize, int maxPoolSize)
        {
            if (minPoolSize <= 0 || maxPoolSize < minPoolSize)
            {
                throw new ArgumentOutOfRangeException(
                    "Minimum connection pool size must be greater than zero and less than or equal to the maximum pool size.",
                    innerException: null);
            }

            currentMinPoolSize = minPoolSize;
            currentMaxPoolSize = maxPoolSize;
        }

        /// <summary>
        /// Gets or creates a connection pool associated with a named pipe.
        /// </summary>
        public static NamedPipeConnectionPool GetConnectionPool(string pipeName)
        {
            NamedPipeConnectionPool pool;
            if (!ConnectionPools.TryGetValue(pipeName, out pool))
            {
                lock (ConnectionPools)
                {
                    pool = ConnectionPools.GetOrAdd(
                        pipeName,
                        name => new NamedPipeConnectionPool(name));
                }
            }

            return pool;
        }

        /// <summary>
        /// Fetches a named-pipe connection from the current connection pool.
        /// </summary>
        public NamedPipeClientStream GetConnection()
        {
            this.ThrowIfDisposed();

            // The connection pool is allocated to the min-size on the first request for a connection
            // by a client.
            if (!this.isConnectionPoolInitialized)
            {
                lock (this.createNewConnectionsLock)
                {
                    if (!this.isConnectionPoolInitialized)
                    {
                        this.availablePool = new BlockingCollection<NamedPipeClientStream>(
                            new ConcurrentStack<NamedPipeClientStream>());
                        this.allConnections = new ConcurrentDictionary<NamedPipeClientStream, object>();

                        this.EnsureMinimumConnections();
                        this.isConnectionPoolInitialized = true;
                    }
                }
            }

            NamedPipeClientStream connection;
            if (!this.TryGetConnectionInternal(out connection))
            {
                throw new TimeoutException("Timed out waiting for an available pipe connection from the pool.");
            }

            if (PipeServerPerformanceCounters.IsInitialized)
            {
                PipeServerPerformanceCounters.ActiveConnections.Increment();
            }

            return connection;
        }

        bool TryGetConnectionInternal(out NamedPipeClientStream connection)
        {
            DateTime waitExpirationTime = DateTime.Now.Add(WaitForConnectionTimeout);
            long startTime = Stopwatch.GetTimestamp();

            // If the connection pool becomes exhausted (e.g. due to overwhelming demand), and an attempt
            // to acquire a connection blocks for at least 100 milliseconds, we add a new connection one-
            // at-a-time until we hit the max pool size. If a client is unable to acquire a connection pipe
            // after waiting for 30 seconds, a timeout exception is thrown.
            bool foundConnection = false;
            while (true)
            {
                foundConnection = this.availablePool.TryTake(out connection, AddConnectionTimeout);
                if (!foundConnection && DateTime.Now < waitExpirationTime)
                {
                    lock (this.createNewConnectionsLock)
                    {
                        // The second attempt to pull from the pool will not block, even if the pool is still empty.
                        // If one isn't found, we'll try to create more.
                        foundConnection = this.availablePool.TryTake(out connection);
                        if (!foundConnection)
                        {
                            // Still no connections? Try creating more.
                            int addedConnections = this.EnsureMinimumConnections();
                            if (addedConnections == 0 && this.allConnections.Count < this.maxPoolSize)
                            {
                                this.AddConnection();
                            }

                            continue;
                        }
                    }
                }

                if (connection != null && !connection.IsConnected)
                {
                    // Connection has gone bad - remove it from the pool and find a new one.
                    this.allConnections.Remove(connection);
                    connection.Dispose();
                    continue;
                }

                Debug.Assert(foundConnection == (connection != null), "Inconsistency between foundConnection and connection != null");

                if (PipeServerPerformanceCounters.IsInitialized)
                {
                    long endTime = Stopwatch.GetTimestamp();
                    PipeServerPerformanceCounters.AverageConnectionWaitTime.IncrementBy(endTime - startTime);
                    PipeServerPerformanceCounters.AverageConnectionWaitTimeBase.Increment();
                }

                return foundConnection;
            }
        }

        public void ReleaseConnection(NamedPipeClientStream connection, bool isBadConnection = false)
        {
            this.ThrowIfDisposed();

            if (!this.isConnectionPoolInitialized || !this.allConnections.ContainsKey(connection))
            {
                return;
            }

            // If the connection is bad, do not add it back to the pool. Note that this condition 
            // can cause the pool size to temporarily drop below the minimum number.
            if (isBadConnection)
            {
                this.allConnections.Remove(connection);
                connection.Dispose();

                if (PipeServerPerformanceCounters.IsInitialized)
                {
                    PipeServerPerformanceCounters.CurrentConnections.Decrement();
                    PipeServerPerformanceCounters.ActiveConnections.Decrement();
                }
            }
            else
            {
                this.availablePool.Add(connection);

                if (PipeServerPerformanceCounters.IsInitialized)
                {
                    PipeServerPerformanceCounters.ActiveConnections.Decrement();
                }
            }
        }

        // This method is expected to be called while holding the createNewConnectionsLock
        int EnsureMinimumConnections()
        {
            int addedConnections = 0;
            while (this.allConnections.Count < this.minPoolSize)
            {
                this.AddConnection();
                addedConnections++;
            }

            return addedConnections;
        }

        // This method is expected to be called while holding the createNewConnectionsLock
        void AddConnection()
        {
            string dataPipeName = this.EstablishNewServerPipe();

            // Now connect to the client-specific pipe that was created by the server.
            var clientPipeStream = NamedPipeHelpers.CreateClientPipeStream(dataPipeName);
            try
            {
                clientPipeStream.Connect(ConnectTimeoutMs);
            }
            catch (Exception)
            {
                if (PipeServerPerformanceCounters.IsInitialized)
                {
                    PipeServerPerformanceCounters.FailedConnections.Increment();
                }

                clientPipeStream.Dispose();
                throw;
            }

            this.availablePool.Add(clientPipeStream);
            this.allConnections.Add(clientPipeStream, null);
            Debug.WriteLine(string.Format("New connection added to the pool. Total: {0}", this.allConnections.Count));

            if (PipeServerPerformanceCounters.IsInitialized)
            {
                PipeServerPerformanceCounters.CurrentConnections.Increment();
            }
        }

        // Suppress inlining so that we have distinct callstacks for the different connection failure scenarios.
        [MethodImpl(MethodImplOptions.NoInlining)]
        string EstablishNewServerPipe()
        {
            using (var handshakePipeStream = NamedPipeHelpers.CreateClientPipeStream(this.handshakePipeName))
            {
                try
                {
                    handshakePipeStream.Connect(ConnectTimeoutMs);
                }
                catch
                {
                    if (PipeServerPerformanceCounters.IsInitialized)
                    {
                        PipeServerPerformanceCounters.FailedConnections.Increment();
                    }

                    throw;
                }

                // Request a the opening of a new client pipe connection.
                PipeMessage.OpenMessage.WriteTo(handshakePipeStream);

                // As part of the handshake, the server responds with the name of a pipe
                // that can be used exclusively by this client for sending/receiving data.
                PipeMessage handshakeResponse = PipeMessage.ReadFrom(handshakePipeStream);
                return handshakeResponse.Data;
            }
        }

        /// <summary>
        /// Disposes the current <see cref="NamedPipeConnectionPool"/> object and attmpts to
        /// close each individual pipe.
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed)
            {
                return;
            }

            NamedPipeConnectionPool ignore;
            ConnectionPools.TryRemove(this.handshakePipeName, out ignore);

            if (this.isConnectionPoolInitialized)
            {
                foreach (NamedPipeClientStream connection in this.allConnections.Keys.ToList())
                {
                    this.allConnections.Remove(connection);
                    connection.Dispose();
                }

                this.availablePool.Dispose();
            }

            this.isDisposed = true;
        }

        void ThrowIfDisposed()
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException(this.GetType().FullName);
            }
        }
    }
}
