namespace PipeServer
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Pipes;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Generic named-pipe server which supports multiple concurrent client connections and request/response messaging patterns.
    /// </summary>
    /// <remarks>
    /// This named pipe server is intended to be a lightweight alternative to WCF net.pipe service hosts. It is designed
    /// for request/response workloads with high-throughput requirements and many concurrent clients. There is a lightweight
    /// messaging protocol used by this server for processing message streams so it is best used in conjuction with the
    /// <see cref="NamedPipeClient"/> on the client-side.
    /// </remarks>
    public class NamedPipeServer : IDisposable
    {
        readonly string pipeName;

        long connectionCounter;
        Func<string, Task<string>> asyncMessageHandler;
        Thread listenThread;
        HashSet<PipeStream> dataStreams;
        bool isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="NamedPipeServer"/> class.
        /// </summary>
        /// <param name="name">
        /// The name of the primary pipe on which new client connections are established.
        /// The name of the pipe may contain at most one forward slash ('/') character.
        /// </param>
        public NamedPipeServer(string name)
        {
            this.pipeName = name;
            this.dataStreams = new HashSet<PipeStream>();
        }

        /// <summary>
        /// Optional handler called when an unhandled exception occurs during message processing.
        /// </summary>
        public Action<Exception> RequestExceptionHandler { get; set; }

        /// <summary>
        /// Opens the specified named pipe and listens for new connections in the background.
        /// </summary>
        public void Listen(Func<string, Task<string>> messageHandler)
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }

            if (this.listenThread != null)
            {
                throw new InvalidOperationException("The server is already in the listening state.");
            }

            this.asyncMessageHandler = messageHandler;

            this.listenThread = new Thread(this.HandshakeListenLoop);
            this.listenThread.Name = string.Format("{0}_{1}", this.GetType().Name, this.pipeName);
            this.listenThread.Start();
        }

        void HandshakeListenLoop()
        {
            while (true)
            {
                // There is one dedicated thread which handles establishing all connections handshakes.
                // The result of the handshake is a new "data" pipe that is dedicated to the connecting client
                // which operates asynchronously on background threads.
                using (NamedPipeServerStream handshakePipe = NamedPipeHelpers.CreateServerPipeStream(this.pipeName))
                {
                    handshakePipe.WaitForConnection();

                    try
                    {
                        PipeMessage message = PipeMessage.ReadFrom(handshakePipe);
                        if (message.IsOpenRequested)
                        {
                            // Send the client the name of a pipe on which the data connection can take place.
                            string dataPipeName = string.Concat(this.pipeName, "#", ++this.connectionCounter);

                            // Start listening on the data pipe name
                            NamedPipeServerStream dataPipe = this.CreateDataPipe(dataPipeName);
                            this.dataStreams.Add(dataPipe);
                            dataPipe.BeginWaitForConnection(this.OnClientConnectionEstablished, dataPipe);

                            PipeMessage handshakeResponse = new PipeMessage(dataPipeName);
                            handshakeResponse.WriteTo(handshakePipe);
                        }
                        else if (message.IsCloseRequested)
                        {
                            // Shutdown has been requested
                            this.CloseAllDataPipes();
                            break;
                        }
                    }
                    catch (IOException e)
                    {
                        if (PipeServerPerformanceCounters.IsInitialized)
                        {
                            PipeServerPerformanceCounters.FailedConnections.Increment();
                        }

                        // Indicates a problem with the handshake. Handle the exception and listen for a new one.
                        this.OnUnhandledException(e);
                    }
                }
            }
        }

        async void OnClientConnectionEstablished(IAsyncResult result)
        {
            NamedPipeServerStream dataPipe = (NamedPipeServerStream)result.AsyncState;

            if (PipeServerPerformanceCounters.IsInitialized)
            {
                PipeServerPerformanceCounters.ActiveConnections.Increment();
            }

            try
            {
                dataPipe.EndWaitForConnection(result);

                // Read the first message and continue reading message until the pipe is closed. Async
                // is important here because no data may ever get written to this pipe if the connection
                // pool associated with this pipe is over-provisioned.
                PipeMessage clientMessage = await PipeMessage.ReadFromAsync(dataPipe);
                while (!clientMessage.IsCloseRequested)
                {
                    // Allow the application code to handle the raw string message and provide a response.
                    string applicationResponseData = await this.asyncMessageHandler(clientMessage.Data);

                    // Write the response back synchronously. The client is always expected to be
                    // able to handle the result immediately and synchronous write-backs have shown
                    // to me much more efficient vs. async based on benchmark testing.
                    PipeMessage responseMessage = new PipeMessage(applicationResponseData);
                    responseMessage.WriteTo(dataPipe);

                    // Read the next message, which could come from the same client or could come
                    // from a new client that is reusing the this pipe from a connection pool.
                    clientMessage = await PipeMessage.ReadFromAsync(dataPipe);
                }
            }
            catch (Exception e)
            {
                if (PipeServerPerformanceCounters.IsInitialized)
                {
                    PipeServerPerformanceCounters.FailedConnections.Increment();
                }

                // Give the host an opportunity to handle the exception, but don't let
                // it go unhandled or else it will bring down the process.
                this.OnUnhandledException(e);
            }
            finally
            {
                if (PipeServerPerformanceCounters.IsInitialized)
                {
                    PipeServerPerformanceCounters.ActiveConnections.Decrement();
                }

                this.CloseDataPipe(dataPipe);
            }
        }

        /// <summary>
        /// Shuts down the current <see cref="NamedPipeServer"/> instance.
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed)
            {
                return;
            }

            // It's not possible to interrupt the call to WaitForConnection() by the handshake pipe
            // so the BCL team recommends interrupting via a protocol message. In this case, we send
            // a "CLOSE" message to the handshake pipe.
            using (var clientPipe = NamedPipeHelpers.CreateClientPipeStream(this.pipeName))
            {
                clientPipe.Connect(timeout: 5000); // 5 seconds
                PipeMessage.CloseMessage.WriteTo(clientPipe);
            }

            if (!this.listenThread.Join(TimeSpan.FromSeconds(5)))
            {
                this.listenThread.Abort();
                this.listenThread.Join();
            }

            // The shutdown message sent earlier should have already done this, but do it again
            // here in case the message delivery fails for any reason.
            this.CloseAllDataPipes();

            this.isDisposed = true;
        }

        void OnUnhandledException(Exception e)
        {
            var exceptionHandler = this.RequestExceptionHandler;
            if (exceptionHandler != null)
            {
                exceptionHandler(e);
            }
        }

        NamedPipeServerStream CreateDataPipe(string pipeName)
        {
            NamedPipeServerStream pipe = NamedPipeHelpers.CreateServerPipeStream(pipeName);
            lock (this.dataStreams)
            {
                this.dataStreams.Add(pipe);
            }

            if (PipeServerPerformanceCounters.IsInitialized)
            {
                PipeServerPerformanceCounters.CurrentConnections.Increment();
            }

            return pipe;
        }

        void CloseDataPipe(NamedPipeServerStream pipe)
        {
            pipe.Dispose();
            lock (this.dataStreams)
            {
                this.dataStreams.Remove(pipe);
            }

            if (PipeServerPerformanceCounters.IsInitialized)
            {
                PipeServerPerformanceCounters.CurrentConnections.Decrement();
            }
        }

        void CloseAllDataPipes()
        {
            foreach (NamedPipeServerStream pipe in this.dataStreams.ToList())
            {
                this.CloseDataPipe(pipe);
            }
        }
    }
}
