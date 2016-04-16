namespace PipeServer
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    /// Thread-safe pool of memory optimized for sending and receiving bytes across I/O devices.
    /// </summary>
    /// <remarks>
    /// Internally the <see cref="BufferPool"/> maintains one or more 4MB byte array (slabs) from which all 
    /// allocation requests are taken. Buffers allocated from the slab are intended to be used for reading and
    /// writing bytes from I/O devices (e.g. named pipes). These devices need to pin the managed byte arrays while
    /// the I/O operation is in progress, which can be expensive in terms of CPU if there is a lot of I/O. To
    /// avoid the CPU overhead of pinning on each I/O operation, we pin the slab once at creation time so that
    /// native code can safely read bytes from it without worrying about the managed garbage collector.
    /// </remarks>
    public sealed class BufferPool : IDisposable
    {
        // 4 MB is a somewhat arbitrary number that ensures each slab is allocated in the Gen2 heap.
        public const int SlabSize = 4 * 1024 * 1024;

        public static readonly BufferPool Default = new BufferPool();
        
        /// <summary>The current slab from which all allocations come.</summary>
        Slab slab;

        /// <summary>A collection of slabs that are ready to be used for allocations.</summary>
        ConcurrentStack<Slab> poolOfSlabs;

        bool isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferPool"/> class.
        /// </summary>
        public BufferPool()
        {
            this.slab = new Slab(SlabSize);
            this.poolOfSlabs = new ConcurrentStack<Slab>();
            this.PoolSize = 1;
        }

        /// <summary>
        /// Gets the number of shared buffers in the current <see cref="BufferPool"/>.
        /// </summary>
        public int PoolSize
        {
            get;
            private set;
        }

        /// <summary>
        /// Returns a <see cref="BufferSegment"/> which represents a reservation of bytes from internally managed buffers.
        /// </summary>
        /// <param name="size">The number of bytes to reserve.</param>
        /// <returns>Returns a segment of a larger buffer.</returns>
        public BufferSegment GetBuffer(int size)
        {
            this.ThrowIfDisposed();

            if (size <= 0)
            {
                throw new ArgumentOutOfRangeException("size");
            }

            // Create a dedicated array for large requests.
            if (size > SlabSize)
            {
                return new BufferSegment(new byte[size]);
            }

            while (true)
            {
                Slab currentSlab = this.slab;

                // Reserve space for the current request and make sure we have enough space available.
                BufferSegment segment;
                if (currentSlab.TryAcquireSegment(size, out segment))
                {
                    return segment;
                }

                // Not enough space; swap in a new slab or one from the pool.
                Slab nextSlab;
                bool isNewSlab = false;
                if (!this.poolOfSlabs.TryPop(out nextSlab))
                {
                    nextSlab = new Slab(SlabSize);
                    isNewSlab = true;
                }

                if (Interlocked.CompareExchange<Slab>(ref this.slab, nextSlab, currentSlab) == currentSlab)
                {
                    if (isNewSlab)
                    {
                        int currentPoolSize = ++this.PoolSize;
                        Debug.WriteLine("Allocated a new slab in the buffer pool. Current pool size = " + currentPoolSize);
                    }
                }
            }
        }

        /// <summary>
        /// Releases a <see cref="BufferSegment"/> that was acquired using <see cref="GetBuffer"/>.
        /// </summary>
        /// <param name="segment">The segment to release.</param>
        public void ReleaseBuffer(BufferSegment segment)
        {
            this.ThrowIfDisposed();

            Slab slab = segment.Slab;
            if (slab == null)
            {
                // This segment isn't being tracked by the buffer pool.
                return;
            }

            bool isFinalRelease = slab.ReleaseSegment(segment);
            if (isFinalRelease)
            {
                this.Recycle(slab);
            }
        }

        public void Dispose()
        {
            if (!this.isDisposed)
            {
                if (this.slab != null)
                {
                    this.slab.Dispose();
                }

                Slab slab;
                while (this.poolOfSlabs.TryPop(out slab))
                {
                    slab.Dispose();
                }

                this.isDisposed = true;
            }
        }

        void Recycle(Slab slab)
        {
            Debug.WriteLine(string.Format("[BufferPool:{0}] Recycling slab {1}", AppDomain.CurrentDomain.FriendlyName, slab.GetHashCode()));
            slab.Reset();
            this.poolOfSlabs.Push(slab);
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
