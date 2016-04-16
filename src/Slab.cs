namespace PipeServer
{
    using System;
    using System.Diagnostics;
    using System.Runtime.InteropServices;
    using System.Threading;

    /// <summary>
    /// Represents a shared, contiguous block of memory from which all buffer allocations are taken.
    /// </summary>
    sealed class Slab : IDisposable
    {
        readonly object initLock = new object();
        readonly int slabSize;

        GCHandle handle;
        byte[] array;
        int head;
        int allocationCount;
        int deallocationCount;
        bool isDisposed;

        public Slab(int size)
        {
            this.slabSize = size;
        }

        /// <summary>
        /// Releases the <see cref="GCHandle"/> associated with the current slab.
        /// </summary>
        ~Slab()
        {
            this.DisposeInternal();
        }

        /// <summary>
        /// Gets a value indicating whether the current slab is finished handing out byte segments.
        /// </summary>
        bool IsDoneAllocating
        {
            get { return this.head >= this.slabSize; }
        }

        /// <summary>
        /// Tries to acquire an exclusive <see cref="BufferSegment"/> from the slab.
        /// </summary>
        /// <param name="size">The number of bytes to reserve.</param>
        /// <param name="segment">If the request succeeded, the segment containing the bytes and the offsets.</param>
        /// <returns>Returns true if the reservation was successful; false otherwise.</returns>
        public bool TryAcquireSegment(int size, out BufferSegment segment)
        {
            this.ThrowIfDisposed();

            // Lazily initialize the array to ensure we only allocate memory for slabs that are actually used.
            this.EnsureInitialized();

            // The head pointer indicates the next available segment of memory.
            int next = Interlocked.Add(ref this.head, size);
            if (next <= this.slabSize)
            {
                Interlocked.Increment(ref this.allocationCount);
                int offset = next - size;
                segment = new BufferSegment(this.array, offset, size, this);
                return true;
            }

            // The slab has been used up
            segment = new BufferSegment();
            return false;
        }

        /// <summary>
        /// Increments the number of bytes deallocated.
        /// </summary>
        /// <param name="segment">A <see cref="BufferSegment"/> that was obtained using <see cref="TryAcquireSegment"/>.</param>
        /// <returns>Returns true if the slab is now able to be reclaimed; false otherwise.</returns>
        public bool ReleaseSegment(BufferSegment segment)
        {
            this.ThrowIfDisposed();

            if (Interlocked.Increment(ref this.deallocationCount) == this.allocationCount)
            {
                // Double-check the state of the slab to ensure that no more threads will attempt
                // to read the bytes in the slab.
                if (this.IsDoneAllocating && this.deallocationCount == this.allocationCount)
                {
                    return true;
                }
            }

            Debug.Assert(this.deallocationCount <= this.allocationCount, "Extra bytes were deallocated. Double-release somewhere?");
            return false;
        }

        /// <summary>
        /// Resets the state of the current <see cref="Slab"/> so that it can be re-used for future allocations.
        /// </summary>
        public void Reset()
        {
            this.ThrowIfDisposed();

            this.head = 0;
            this.allocationCount = 0;
            this.deallocationCount = 0;
        }

        public void Dispose()
        {
            if (!this.isDisposed)
            {
                this.DisposeInternal();

                GC.SuppressFinalize(this);
                this.isDisposed = true;
            }
        }

        void DisposeInternal()
        {
            if (this.handle.IsAllocated)
            {
                Debug.WriteLine("[BufferPool:{0}] Deallocating slab {1}", AppDomain.CurrentDomain.FriendlyName, this.GetHashCode());
                this.handle.Free();
            }
        }

        void EnsureInitialized()
        {
            if (this.array == null)
            {
                lock (this.initLock)
                {
                    if (this.array == null)
                    {
                        Debug.WriteLine("[BufferPool:{0}] Allocating slab {1}", AppDomain.CurrentDomain.FriendlyName, this.GetHashCode());
                        this.array = new byte[this.slabSize];
                        this.handle = GCHandle.Alloc(this.array, GCHandleType.Pinned);
                    }
                }
            }
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
