using System.Net;
using System.Net.Sockets;

public static class RecvHelper
{
    public static byte[] ReceiveNonBlocking(UdpClient udp, ref IPEndPoint receiveAddr)
    {
        try
        {
            return udp.Receive(ref receiveAddr);
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.WouldBlock)
            {
                return null;
            }

            if (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                return null;
            }

            throw;
        }
    }
}