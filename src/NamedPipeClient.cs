namespace PipeServer
{
    using System;
    using System.IO;
    using System.IO.Pipes;
    using System.Threading.Tasks;

    /// <summary>
    /// A client for the <see cref="NamedPipeServer"/>.
    /// </summary>
    /// <remarks>
    /// The <see cref="NamedPipeClient"/> uses connection pooling to establish connections with the
    /// target <see cref="NamedPipeServer"/>. This is intended to improve overall system performance
    /// by limiting the number of actual connections that take place. A connection pool is identified
    /// by the name of the pipe given to the client constructor and all clients in the same app domain
    /// targeting the same named pipe will use the same connection pool. Each client instance is given
    /// exclusive access to its individual connection until <see cref="Dispose"/> is called, at which
    /// point the connection is returned back to the connection pool and can be reused by another client.
    /// </remarks>
    public class NamedPipeClient : IDisposable
    {
        readonly NamedPipeConnectionPool connectionPool;

        NamedPipeClientStream clientPipeStream;

        /// <summary>
        /// Initializes a new instance of the <see cref="NamedPipeClient"/> class intended to
        /// communicate with a <see cref="NamedPipeServer"/> which is listening on <paramref name="pipeName"/>.
        /// </summary>
        /// <remarks>
        /// The <param name="pipeName"/> parameter value should match the value specified on the server.
        /// This pipe is used for establishing a logical connection to the pipe server, but the actual
        /// data will flow to the server using a seperate pipe that gets created at client-connection time.
        /// </remarks>
        public NamedPipeClient(string pipeName)
        {
            this.connectionPool = NamedPipeConnectionPool.GetConnectionPool(pipeName);
        }

        /// <summary>
        /// Establishes a connection with the named pipe server.
        /// </summary>
        /// <remarks>
        /// If this is the first time a connection has been established using this pipe name in the current
        /// app domain, then this call will cause a pool of connections to be created. Otherwise, the client
        /// will take an existing connection from the existing connection pool.
        /// </remarks>
        public void Connect()
        {
            // Acquire a connection from the connection pool
            this.clientPipeStream = this.connectionPool.GetConnection();
        }

        /// <summary>
        /// Synchronously sends data over a named pipe to the named pipe server and returns the server response.
        /// </summary>
        /// <remarks>
        /// This method internally calls <see cref="Connect"/> if a connection has not yet been established
        /// using this <see cref="NamedPipeClient"/> instance.
        /// </remarks>
        public string Send(string data)
        {
            if (this.clientPipeStream == null)
            {
                this.Connect();
            }

            try
            {
                PipeMessage message = new PipeMessage(data);
                message.WriteTo(this.clientPipeStream);
                PipeMessage response = PipeMessage.ReadFrom(this.clientPipeStream);
                return response.Data;
            }
            catch (IOException)
            {
                // The connection may have broken, so release it.
                this.ReleaseConnection(isBadConnection: true);
                throw;
            }
        }

        /// <summary>
        /// Asynchronously sends data over a named pipe to the named pipe server and returns the server response.
        /// </summary>
        /// <remarks>
        /// This method internally calls <see cref="Connect"/> if a connection has not yet been established
        /// using this <see cref="NamedPipeClient"/> instance.
        /// </remarks>
        public async Task<string> SendAsync(string data)
        {
            if (this.clientPipeStream == null)
            {
                // TODO: Make this async when upgrading to .NET 4.6 (4.5 doesn't support async client connection).
                this.Connect();
            }

            PipeMessage message = new PipeMessage(data);
            try
            {
                await message.WriteToAsync(this.clientPipeStream);
                PipeMessage response = await PipeMessage.ReadFromAsync(this.clientPipeStream);
                return response.Data;
            }
            catch (IOException)
            {
                // The connection may have broken, so release it.
                this.ReleaseConnection(isBadConnection: true);
                throw;
            }
        }

        /// <summary>
        /// Releases any connection established with the pipe server and cleans up instance state.
        /// </summary>
        public void Dispose()
        {
            this.ReleaseConnection(isBadConnection: false);
        }

        void ReleaseConnection(bool isBadConnection)
        {
            if (this.clientPipeStream != null)
            {
                // Release it back to the pool and remove our reference to it.
                NamedPipeClientStream pipeToRelease = this.clientPipeStream;
                this.clientPipeStream = null;
                this.connectionPool.ReleaseConnection(pipeToRelease, isBadConnection);
            }
        }
    }
}
