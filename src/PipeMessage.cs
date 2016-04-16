namespace PipeServer
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Pipes;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a message sent between a <see cref="NamedPipeClient"/> and <see cref="NamedPipeServer"/>.
    /// </summary>
    /// <remarks>
    /// Messages that flow between the client and server are expected to be in the following format:
    /// 
    /// {protocol-version(2)}{payload-size(4)}{payload-data(N)}
    /// 
    /// As mentioned above, each message internally contains a protocol version in the first two bytes,
    /// followed by a payload size in the next four bytes, followed by a payload of arbitrary size.
    /// </remarks>
    struct PipeMessage
    {
        // NOTE: When making changes to the message protocol, the version number must be incremented
        //       and the version specific handling should be ADDED to the ReadFrom method so that both
        //       old a new versions of the protocol can be supported side-by-side.
        const ushort DefaultVersion = 1;

        public static readonly PipeMessage OpenMessage = new PipeMessage("OPEN");
        public static readonly PipeMessage CloseMessage = new PipeMessage("CLOSE");

        readonly string data;

        public PipeMessage(string data)
        {
            this.data = data;
        }

        public string Data
        {
            get { return this.data; }
        }

        public bool IsOpenRequested
        {
            get { return this.data == OpenMessage.Data; }
        }

        public bool IsCloseRequested
        {
            get { return this.data == CloseMessage.Data; }
        }

        public void WriteTo(PipeStream pipeStream)
        {
            // Sends the entire message in a single PipeStream.Write() call onto the pipe to avoid chattiness.
            BufferSegment buffer = this.PrepareDataForWriting();
            try
            {
                pipeStream.Write(buffer.Array, buffer.Offset, buffer.Count);
                pipeStream.Flush();
            }
            finally
            {
                BufferPool.Default.ReleaseBuffer(buffer);
            }
        }

        public Task WriteToAsync(PipeStream pipeStream)
        {
            return this.WriteToAsync(pipeStream, CancellationToken.None);
        }

        public async Task WriteToAsync(PipeStream pipeStream, CancellationToken cancellationToken)
        {
            // Send the entire message in a single PipeStream.Write() call onto the pipe to avoid chattiness.
            BufferSegment buffer = this.PrepareDataForWriting();
            try
            {
                await pipeStream.WriteAsync(buffer.Array, buffer.Offset, buffer.Count, cancellationToken);
                await pipeStream.FlushAsync();
            }
            finally
            {
                BufferPool.Default.ReleaseBuffer(buffer);
            }
        }

        BufferSegment PrepareDataForWriting()
        {
            int sizeOfPayload = Encoding.UTF8.GetByteCount(this.data);
            int messageSize = sizeof(ushort) + sizeof(int) + sizeOfPayload;

            BufferSegment buffer = BufferPool.Default.GetBuffer(messageSize);
            
            int offset = buffer.Offset;
            offset += WriteToBuffer(DefaultVersion, buffer.Array, offset);
            offset += WriteToBuffer(sizeOfPayload, buffer.Array, offset);
            Encoding.UTF8.GetBytes(this.data, 0, this.data.Length, buffer.Array, offset);

            return buffer;
        }

        static int WriteToBuffer(ushort value, byte[] buffer, int offset)
        {
            buffer[offset + 0] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            return 2;
        }

        static int WriteToBuffer(int value, byte[] buffer, int offset)
        {
            buffer[offset + 0] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
            return 4;
        }

        public static PipeMessage ReadFrom(PipeStream pipe)
        {
            PipeMessage message;
            try
            {
                int? messageVersion = ReadProtocolVersion(pipe);
                message = ReadFromInternal(pipe, messageVersion);
            }
            catch (IOException e)
            {
                // This is expected to be caused by a pipe being closed unexpectedly. We handle
                // it in the same way as a graceful shutdown of the pipe connection.
                Debug.WriteLine(string.Format("Exception reading from the pipe: {0}", e));
                message = CloseMessage;
            }

            if (message.data == null)
            {
                throw new InvalidDataException("The message obtained from the stream is not recognizeable");
            }

            return message;
        }

        public static async Task<PipeMessage> ReadFromAsync(PipeStream pipe)
        {
            PipeMessage message;
            try
            {
                int? messageVersion = await ReadProtocolVersionAsync(pipe);
                message = ReadFromInternal(pipe, messageVersion);
            }
            catch (IOException e)
            {
                // This is expected to be caused by a pipe being closed unexpectedly. We handle
                // it in the same way as a graceful shutdown of the pipe connection.
                Debug.WriteLine(string.Format("Exception reading from the pipe: {0}", e));
                message = CloseMessage;
            }

            if (message.data == null)
            {
                throw new InvalidDataException("The message obtained from the stream is not recognizeable");
            }

            return message;
        }

        static PipeMessage ReadFromInternal(PipeStream pipe, int? messageVersion)
        {
            if (messageVersion == null)
            {
                // This is expected during an app-domain shutdown.
                return CloseMessage;
            }

            if (messageVersion == 1)
            {
                // First, read the length of the received data payload.
                // CONSIDER: Allow the receiver to provide a maximum message size as a security mechanism.
                int dataLength;
                BufferSegment dataLengthBuffer = BufferPool.Default.GetBuffer(sizeof(int));
                try
                {
                    pipe.Read(dataLengthBuffer.Array, dataLengthBuffer.Offset, dataLengthBuffer.Count);
                    dataLength = BitConverter.ToInt32(dataLengthBuffer.Array, dataLengthBuffer.Offset);
                }
                finally
                {
                    BufferPool.Default.ReleaseBuffer(dataLengthBuffer);
                }

                // Read the data payload.
                string data;
                BufferSegment dataPayloadBuffer = BufferPool.Default.GetBuffer(dataLength);
                try
                {
                    pipe.Read(dataPayloadBuffer.Array, dataPayloadBuffer.Offset, dataPayloadBuffer.Count);
                    data = Encoding.UTF8.GetString(dataPayloadBuffer.Array, dataPayloadBuffer.Offset, dataPayloadBuffer.Count);
                }
                finally
                {
                    BufferPool.Default.ReleaseBuffer(dataPayloadBuffer);
                }

                return new PipeMessage(data);
            }
            else
            {
                throw new InvalidDataException(string.Format("A message was received with an unknown version number: {0}", messageVersion));
            }
        }

        static int? ReadProtocolVersion(PipeStream stream)
        {
            // The version number in the first two bytes of the stream inform how we should read the message.
            // WARNING: The version protocol must never be changed!
            BufferSegment versionBuffer = BufferPool.Default.GetBuffer(sizeof(ushort));
            try
            {
                int bytesRead = stream.Read(versionBuffer.Array, versionBuffer.Offset, versionBuffer.Count);
                if (bytesRead == 0)
                {
                    // PipeStream.Read() returns without any data during a shutdown.
                    return null;
                }
            
                return BitConverter.ToUInt16(versionBuffer.Array, versionBuffer.Offset);
            }
            finally
            {
                BufferPool.Default.ReleaseBuffer(versionBuffer);
            }
        }

        static async Task<int?> ReadProtocolVersionAsync(PipeStream stream)
        {
            // The version number in the first two bytes of the stream inform how we should read the message.
            // WARNING: The version protocol must never be changed!
            BufferSegment versionBytes = BufferPool.Default.GetBuffer(sizeof(ushort));
            try
            {
                int bytesRead = await stream.ReadAsync(versionBytes.Array, versionBytes.Offset, versionBytes.Count);
                if (bytesRead == 0)
                {
                    // PipeStream.Read() returns without any data during a shutdown.
                    return null;
                }

                return BitConverter.ToUInt16(versionBytes.Array, versionBytes.Offset);
            }
            finally
            {
                BufferPool.Default.ReleaseBuffer(versionBytes);
            }
        }
    }
}
