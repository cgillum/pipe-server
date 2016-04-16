namespace PipeServer
{
    /// <summary>
    /// Represents a segment of a larger byte buffer, similar to <see cref="ArraySegment{byte}"/>.
    /// </summary>
    public struct BufferSegment
    {
        byte[] array;
        int offset;
        int count;
        Slab slab;

        internal BufferSegment(byte[] array, int offset, int count, Slab slab)
        {
            this.array = array;
            this.slab = slab;
            this.offset = offset;
            this.count = count;
        }

        internal BufferSegment(byte[] array)
        {
            this.array = array;
            this.slab = null;
            this.offset = 0;
            this.count = array.Length;
        }

        public byte[] Array
        {
            get { return this.array; }
        }

        public int Offset
        {
            get { return this.offset; }
        }

        public int Count
        {
            get { return this.count; }
        }

        internal Slab Slab
        {
            get { return this.slab; }
        }
    }
}
