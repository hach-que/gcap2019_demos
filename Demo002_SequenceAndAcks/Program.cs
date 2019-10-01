using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Demo002_Connections
{
    /*
    
        NOTE: This protocol is for educational purposes only. You should not use
        this protocol in a production application.
    
    */

    delegate void MessageReceived(IPEndPoint fromAddr, byte[] message);

    delegate void ClientDisconnected(IPEndPoint addr);

    delegate void ClientConnected(IPEndPoint addr);

    delegate void MessageAcked(IPEndPoint sentToAddr, byte[] message);

    delegate void MessageLost(IPEndPoint sentToAddr, byte[] message);

    class GlobalConfig
    {
        public const int TICK_RATE = 20;
        public const int CONNECTION_TIMEOUT_SECONDS = 2; // just for demo, you'd normally use something much higher
        public const int MAX_MTU = 1500;
    }

    class NetworkConnection
    {
        private readonly UdpClient _udp;
        private readonly IPEndPoint _target;
        private readonly Queue<byte[]> _sendQueue;
        private readonly MessageReceived _onReceive;
        private readonly MessageAcked _onAck;
        private readonly MessageLost _onLost;
        private readonly ClientDisconnected _onDisconnect;
        private int _sendQueueSize;
        private int _timeoutAccumulator;
        private bool _connected;
        private ushort _sendSeq;
        private ushort _ackSeq;
        private uint _ackBitField;
        private Dictionary<ushort, byte[][]> _pendingSendAcks;
        private const int PACKET_HEADER_SIZE_BYTES = sizeof(ushort) * 2 + sizeof(uint);
        private const int MESSAGE_HEADER_SIZE_BYTES = sizeof(ushort);

        public NetworkConnection(UdpClient udp, IPEndPoint target, MessageReceived onReceive, ClientDisconnected onDisconnect, MessageAcked onAck, MessageLost onLost)
        {
            _udp = udp;
            _target = target;
            _sendQueue = new Queue<byte[]>();
            _onReceive = onReceive;
            _onAck = onAck;
            _onLost = onLost;
            _onDisconnect = onDisconnect;
            _connected = true;
            _timeoutAccumulator = 0;
            _sendSeq = 0;
            _ackSeq = 0;
            _ackBitField = 0;
            _sendQueueSize = PACKET_HEADER_SIZE_BYTES;
            _pendingSendAcks = new Dictionary<ushort, byte[][]>();
        }

        public void EnqueueReceive(IPEndPoint fromAddr, byte[] packet)
        {
            // Reset timeout accumulator - we just got a packet.
            _timeoutAccumulator = 0;
            _connected = true;

            // Read the sequence, ack and ack bitfield. In this case, the sequence
            // is the remote connection's sequence - and thus the sequence number
            // we will be acknowledging. Meanwhile the acks are for the messages
            // that we sent out.
            var remoteSeq = BitConverter.ToUInt16(packet, 0);
            var remoteAck = BitConverter.ToUInt16(packet, sizeof(ushort));
            var remoteAckBitfield = BitConverter.ToUInt32(packet, sizeof(ushort) * 2);

            /*******
             * 
             * WARNING! This code does not handle sequence number overflow. Refer to
             * https://web.archive.org/web/20181107181433/https://gafferongames.com/post/reliability_ordering_and_congestion_avoidance_over_udp/#handling-sequence-number-wrap-around
             * for information on how to handle this.
             * 
             ********/

            // Update our acknowledgement of packets the remote machine sent.
            var lastGot = _ackSeq;
            var nowGot = remoteSeq;
            if (nowGot == lastGot)
            {
                // Remote acknowledgement is in the same state, nothing to do.
            }
            else
            {
                // We need to move the acknowledgement of the packet we last got
                // into the bitfield first.
                _ackBitField <<= 1;
                _ackBitField |= 0x1;

                // Shift bitfield across for all the packets we lost in between.
                if (nowGot - lastGot > 1)
                {
                    _ackBitField <<= (nowGot - lastGot) - 1;
                }
            }
            _ackSeq = remoteSeq;

            // Read the acknowledgement data from the remote machine about packets we sent.
            if (_pendingSendAcks.ContainsKey(remoteAck))
            {
                // If it's in the ack field, the remote machine received it.
                foreach (var message in _pendingSendAcks[remoteAck])
                {
                    _onAck(_target, message);
                }
                _pendingSendAcks.Remove(remoteAck);
            }
            for (var i = 0; i < sizeof(uint); i++)
            {
                var seqInBitfield = (ushort)(remoteAck - i - 1);
                var isAcked = ((remoteAckBitfield >> i) & 0x1) == 0x1;
                if (_pendingSendAcks.ContainsKey(seqInBitfield))
                {
                    foreach (var message in _pendingSendAcks[seqInBitfield])
                    {
                        if (isAcked)
                        {
                            _onAck(_target, message);
                        }
                        else
                        {
                            _onLost(_target, message);
                        }
                    }
                }
                _pendingSendAcks.Remove(seqInBitfield);
            }

            // Take the raw packet and split it out into the messages it contains.
            var offset = PACKET_HEADER_SIZE_BYTES;
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
            var dataLength = MESSAGE_HEADER_SIZE_BYTES + message.Length;

            if (dataLength > GlobalConfig.MAX_MTU)
            {
                throw new Exception("message size > " + (GlobalConfig.MAX_MTU - 2) + " bytes");
            }

            if (dataLength + _sendQueueSize > GlobalConfig.MAX_MTU)
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
            // Create buffer for outgoing packet.
            var buffer = new byte[GlobalConfig.MAX_MTU];

            // Write sequence, ack and ack bitfield.
            var sendSeqBytes = BitConverter.GetBytes(_sendSeq);
            var ackSeqBytes = BitConverter.GetBytes(_ackSeq);
            var ackBitfieldBytes = BitConverter.GetBytes(_ackBitField);
            Array.Copy(sendSeqBytes, 0, buffer, 0, sizeof(ushort));
            Array.Copy(ackSeqBytes, 0, buffer, sizeof(ushort), sizeof(ushort));
            Array.Copy(ackBitfieldBytes, 0, buffer, sizeof(ushort) * 2, sizeof(uint));

            // Write message data.
            var offset = PACKET_HEADER_SIZE_BYTES;
            foreach (var item in _sendQueue)
            {
                var lenBytes = BitConverter.GetBytes((ushort)item.Length);
                Array.Copy(lenBytes, 0, buffer, offset, sizeof(ushort));
                Array.Copy(item, 0, buffer, offset + MESSAGE_HEADER_SIZE_BYTES, item.Length);
                offset += 2 + item.Length;
            }

            // Add messages to "pending acks" - messages we've sent but are waiting
            // to see if the remote machine acks them or not.
            _pendingSendAcks.Add(_sendSeq, _sendQueue.ToArray());

            // Send packet.
            _udp.Send(buffer, offset, _target);
            _sendQueue.Clear();
            _sendQueueSize = PACKET_HEADER_SIZE_BYTES;
            _sendSeq++;
        }

        // Call Update at a regular interval.
        public void Update()
        {
            _timeoutAccumulator++;

            if (_connected && _timeoutAccumulator > GlobalConfig.TICK_RATE * GlobalConfig.CONNECTION_TIMEOUT_SECONDS)
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
        private readonly NetworkConnection _server;

        public GameClient(string address, int port, MessageReceived onReceive, ClientDisconnected onTimeout, MessageAcked onAck, MessageLost onLost)
        {
            _udp = new UdpClient(0) { Client = { Blocking = false } };
            _target = new IPEndPoint(IPAddress.Parse(address), port);
            _server = new NetworkConnection(_udp, _target, onReceive, onTimeout, onAck, onLost);
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
        private readonly Dictionary<IPEndPoint, NetworkConnection> _clients;
        private readonly MessageReceived _onReceive;
        private readonly ClientConnected _onClientConnected;
        private readonly ClientDisconnected _onClientDisconnected;
        private readonly MessageAcked _onAck;
        private readonly MessageLost _onLost;

        public GameServer(int port, MessageReceived onReceive, ClientConnected onClientConnected, ClientDisconnected onClientDisconnected, MessageAcked onAck, MessageLost onLost)
        {
            _udp = new UdpClient(port) { Client = { Blocking = false } };
            _clients = new Dictionary<IPEndPoint, NetworkConnection>();
            _onReceive = onReceive;
            _onClientConnected = onClientConnected;
            _onClientDisconnected = onClientDisconnected;
            _onAck = onAck;
            _onLost = onLost;
        }

        public void Send(IPEndPoint clientAddr, byte[] message)
        {
            if (!_clients.ContainsKey(clientAddr))
            {
                // This isn't a client the server has spoken to before; this kind of
                // server-initiated connection doesn't make a lot of sense, but eh,
                // it's not hard to support.
                _clients[clientAddr] = new NetworkConnection(_udp, clientAddr, _onReceive, (fromAddr) => {
                    _clients.Remove(fromAddr);
                    _onClientDisconnected(fromAddr);
                }, _onAck, _onLost);
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
                    _clients[receiveAddr] = new NetworkConnection(_udp, receiveAddr, _onReceive, (fromAddr) => {
                        _clients.Remove(fromAddr);
                        _onClientDisconnected(fromAddr);
                    }, _onAck, _onLost);
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
                }, (sentToAddr, message) =>
                {
                    // On ack.
                }, (sentToAddr, message) =>
                {
                    // This makes it harder to read the output for the purposes of the demo, but you can uncomment
                    // it if you want to see when data sent back to the client is lost.
                    // Console.WriteLine("[server] Message got lost in transit back to client: " + Encoding.ASCII.GetString(message));
                });

                Console.WriteLine("[server] Started");

                while (true)
                {
                    // Update server.
                    server.Update();

                    // Tick.
                    Thread.Sleep(1000 / GlobalConfig.TICK_RATE);
                }
            }
            else
            {
                var running = true;
                var client = new GameClient("127.0.0.1", 19000, (from, data) =>
                {
                    // This makes it harder to read the output for the purposes of the demo, but you can uncomment
                    // it if you want to see the data being echoed back to the client.
                    // Console.WriteLine("[client] Received message from server - " + Encoding.ASCII.GetString(data));
                }, (addr) => {
                    Console.WriteLine("[client] Lost connection to server");
                    running = false;
                }, (sentToAddr, message) =>
                {
                    // On ack.
                }, (sentToAddr, message) =>
                {
                    Console.WriteLine("[client] Message got lost in transit to server: " + Encoding.ASCII.GetString(message));
                });

                Console.WriteLine("[client] Started");

                var i = 0;
                while (running)
                {
                    // Enqueue send.
                    var msg = "Hello World " + i++;
                    Console.WriteLine("[client] Sent message to server - " + msg);
                    client.Send(Encoding.ASCII.GetBytes(msg));

                    // Update client.
                    client.Update();

                    // Tick.
                    Thread.Sleep(1000 / GlobalConfig.TICK_RATE);
                }
            }
        }
    }
}
