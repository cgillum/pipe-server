namespace PipeServer
{
    using System.IO.Pipes;
    using System.Security.AccessControl;
    using System.Security.Principal;

    static class NamedPipeHelpers
    {
        public const int DefaultBufferSize = 4096;

        public static NamedPipeServerStream CreateServerPipeStream(string pipeName)
        {
            // Restrict access to locally authenticated users (S-1-2-0), which is the same access level used by WCF.
            // http://msdn.microsoft.com/en-us/library/cc980032.aspx
            SecurityIdentifier authenticatedUsersSid = new SecurityIdentifier(WellKnownSidType.LocalSid, null);
            PipeSecurity localAccountsSecurity = new PipeSecurity();
            localAccountsSecurity.AddAccessRule(
                new PipeAccessRule(authenticatedUsersSid, PipeAccessRights.ReadWrite, AccessControlType.Allow));

            return new NamedPipeServerStream(
                pipeName,
                PipeDirection.InOut,
                1 /* maxNumberOfServerInstances */,
                PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous,
                DefaultBufferSize /* inBufferSize */,
                DefaultBufferSize /* outBufferSize */,
                localAccountsSecurity);
        }

        public static NamedPipeClientStream CreateClientPipeStream(string pipeName)
        {
            // Client pipes are intentionally non-async, because performance testing has shown
            // a dramatic improvement when going non-async.
            return new NamedPipeClientStream(
                "." /* localhost */,
                pipeName,
                PipeDirection.InOut);
        }
    }
}
