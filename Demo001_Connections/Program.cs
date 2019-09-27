using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Demo001_Connections
{
    /*
    
        NOTE: This protocol is for educational purposes only. You should not use
        this protocol in a production application.
    
    */

    delegate void ReceivePacket(IPEndPoint fromAddr, byte[] message);

    delegate void ClientDisconnected(IPEndPoint addr);

    delegate void ClientConnected(IPEndPoint addr);

    class GlobalConfig
    {
        public const int TICK_RATE = 5;
    }

    class NetworkProtocol
    {
        private readonly UdpClient _udp;
        private readonly IPEndPoint _target;
        private readonly Queue<byte[]> _sendQueue;
        private readonly ReceivePacket _onReceive;
        private readonly ClientDisconnected _onDisconnect;
        private int _sendQueueSize = 0;
        private int _timeoutAccumulator;
        private bool _connected;
        const int MAX_MTU = 1500;

        public NetworkProtocol(UdpClient udp, IPEndPoint target, ReceivePacket onReceive, ClientDisconnected onDisconnect)
        {
            _udp = udp;
            _target = target;
            _sendQueue = new Queue<byte[]>();
            _onReceive = onReceive;
            _onDisconnect = onDisconnect;
            _connected = true;
            _timeoutAccumulator = 0;
        }

        public void EnqueueReceive(IPEndPoint fromAddr, byte[] packet)
        {
            // Reset timeout accumulator - we just got a packet.
            _timeoutAccumulator = 0;
            _connected = true;

            // Take the raw packet and split it out into the messages it contains.
            var offset = 0;
            while (offset < packet.Length)
            {
                var len = BitConverter.ToUInt16(packet, offset);
                offset += 2;

                var message = new byte[len];
                Array.Copy(packet, offset, message, 0, len);
                offset += len;

                _onReceive(fromAddr, message);
            }
        }

        public void EnqueueSend(byte[] message)
        {
            var dataLength = 2 /* uint16 length */ + message.Length;

            if (dataLength > MAX_MTU)
            {
                throw new Exception("message size > " + (MAX_MTU - 2) + " bytes");
            }

            if (dataLength + _sendQueueSize > MAX_MTU)
            {
                // Adding this message to the current queue would cause the
                // pending packet to be greater than MTU; flush the current
                // messages out now.
                FlushSendQueue();
            }

            // Append the packet to the send queue.
            _sendQueue.Enqueue(message);
            _sendQueueSize += dataLength;
        }

        // Takes all of the queues messages and sends them out
        // as a UDP packet.
        private void FlushSendQueue()
        {
            // Get all of the pending data.
            var data = _sendQueue.ToArray();

            // Create buffer for outgoing packet.
            var buffer = new byte[MAX_MTU];
            var offset = 0;
            foreach (var item in _sendQueue)
            {
                var lenBytes = BitConverter.GetBytes((UInt16)item.Length);
                Array.Copy(lenBytes, 0, buffer, offset, 2);
                Array.Copy(item, 0, buffer, offset + 2, item.Length);
                offset += 2 + item.Length;
            }

            // Send packet.
            _udp.Send(buffer, offset, _target);
            _sendQueue.Clear();
            _sendQueueSize = 0;
        }

        // Call Update at a regular interval.
        public void Update()
        {
            _timeoutAccumulator++;

            if (_connected && _timeoutAccumulator > GlobalConfig.TICK_RATE /* hz */ * 10 /* seconds */)
            {
                // This connection has timed out.
                _onDisconnect(_target);
                _connected = false;
            }

            // Always flush the queue and send a packet, even if we have no
            // data pending. This keeps the connection alive due to the presence
            // of packets received on the other end.
            FlushSendQueue();
        }
    }

    class GameClient
    {
        private readonly UdpClient _udp;
        private readonly IPEndPoint _target;
        private readonly NetworkProtocol _server;

        public GameClient(string address, int port, ReceivePacket onReceive, ClientDisconnected onTimeout)
        {
            _udp = new UdpClient(0) { Client = { Blocking = false } };
            _target = new IPEndPoint(IPAddress.Parse(address), port);
            _server = new NetworkProtocol(_udp, _target, onReceive, onTimeout);
        }

        public void Send(byte[] message)
        {
            _server.EnqueueSend(message);
        }

        public void Update()
        {
            while (true)
            {
                var receiveAddr = new IPEndPoint(IPAddress.Loopback, 0);
                var packet = RecvHelper.ReceiveNonBlocking(_udp, ref receiveAddr);
                if (packet == null)
                {
                    break;
                }

                if (!receiveAddr.Equals(_target))
                {
                    // Packet was not from the server.
                    continue;
                }

                _server.EnqueueReceive(receiveAddr, packet);
            }

            _server.Update();
        }
    }

    class GameServer
    {
        private readonly UdpClient _udp;
        private readonly Dictionary<IPEndPoint, NetworkProtocol> _clients;
        private readonly ReceivePacket _onReceive;
        private readonly ClientConnected _onClientConnected;
        private readonly ClientDisconnected _onClientDisconnected;

        public GameServer(int port, ReceivePacket onReceive, ClientConnected onClientConnected, ClientDisconnected onClientDisconnected)
        {
            _udp = new UdpClient(port) { Client = { Blocking = false } };
            _clients = new Dictionary<IPEndPoint, NetworkProtocol>();
            _onReceive = onReceive;
            _onClientConnected = onClientConnected;
            _onClientDisconnected = onClientDisconnected;
        }

        public void Send(IPEndPoint clientAddr, byte[] message)
        {
            if (!_clients.ContainsKey(clientAddr))
            {
                // This isn't a client the server has spoken to before; this kind of
                // server-initiated connection doesn't make a lot of sense, but eh,
                // it's not hard to support.
                _clients[clientAddr] = new NetworkProtocol(_udp, clientAddr, _onReceive, (fromAddr) => {
                    _clients.Remove(fromAddr);
                    _onClientDisconnected(fromAddr);
                });
                _onClientConnected(clientAddr);
            }

            _clients[clientAddr].EnqueueSend(message);
        }

        public void Update()
        {
            // Process incoming packets from clients.
            while (true)
            {
                var receiveAddr = new IPEndPoint(IPAddress.Loopback, 0);
                var packet =  RecvHelper.ReceiveNonBlocking(_udp, ref receiveAddr);
                if (packet == null)
                {
                    break;
                }

                if (!_clients.ContainsKey(receiveAddr))
                {
                    // This is a new client.
                    _clients[receiveAddr] = new NetworkProtocol(_udp, receiveAddr, _onReceive, (fromAddr) => {
                        _clients.Remove(fromAddr);
                        _onClientDisconnected(fromAddr);
                    });
                    _onClientConnected(receiveAddr);
                }

                _clients[receiveAddr].EnqueueReceive(receiveAddr, packet);
            }

            // Process all pending send operations for connected clients.
            foreach (var kv in _clients.ToArray())
            {
                kv.Value.Update();
            }
        }

    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length >= 1 && args[0] == "server")
            {
                GameServer server = null;
                server = new GameServer(19000, (from, data) => {
                    // Server just echos data back to the client.
                    Console.WriteLine("[server] Got message - " + from + " - " + Encoding.ASCII.GetString(data));
                    server.Send(from, data);
                }, (addr) => {
                    Console.WriteLine("[server] Client connected - " + addr);
                }, (addr) => {
                    Console.WriteLine("[server] Client disconnected - " + addr);
                });

                Console.WriteLine("[server] Started");

                while (true)
                {
                    // Update server.
                    server.Update();

                    // Wait to match 20hz.
                    Thread.Sleep(1000 / GlobalConfig.TICK_RATE);
                }
            }
            else
            {
                var client = new GameClient("127.0.0.1", 19000, (from, data) =>
                {
                    // This makes it harder to read the output for the purposes of the demo, but you can uncomment
                    // it if you want to see the data being echoed back to the client.
                    // Console.WriteLine("[client] Received message from server - " + Encoding.ASCII.GetString(data));
                }, (addr) => {
                    Console.WriteLine("[client] Lost connection to server");
                });

                Console.WriteLine("[client] Started");

                var i = 0;
                while (true)
                {
                    // Enqueue send.
                    var msg = "Hello World " + i++;
                    Console.WriteLine("[client] Sent message to server - " + msg);
                    client.Send(Encoding.ASCII.GetBytes(msg));

                    // Update client.
                    client.Update();

                    // Wait to match 20hz.
                    Thread.Sleep(1000 / GlobalConfig.TICK_RATE);
                }
            }
        }
    }
}
