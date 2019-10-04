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
        private int _sendQueueSize;
        private int _timeoutAccumulator;
        private bool _connected;
        private ushort _sendSeq;
        private ushort _ackSeq;
        private uint _ackBitField;
        private Dictionary<ushort, byte[][]> _pendingSendAcks;
        public const int PACKET_HEADER_SIZE_BYTES = sizeof(ushort) * 2 + sizeof(uint);
        public const int MESSAGE_HEADER_SIZE_BYTES = sizeof(ushort);

        public event MessageReceived MessageReceived;
        public event ClientDisconnected ClientDisconnected;
        public event MessageAcked MessageAcked;
        public event MessageLost MessageLost;

        public NetworkConnection(UdpClient udp, IPEndPoint target)
        {
            _udp = udp;
            _target = target;
            _sendQueue = new Queue<byte[]>();
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
                    MessageAcked?.Invoke(_target, message);
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
                            MessageAcked?.Invoke(_target, message);
                        }
                        else
                        {
                            MessageLost?.Invoke(_target, message);
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

                MessageReceived?.Invoke(fromAddr, message);
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
                ClientDisconnected?.Invoke(_target);
                _connected = false;
            }

            // Always flush the queue and send a packet, even if we have no
            // data pending. This keeps the connection alive due to the presence
            // of packets received on the other end.
            FlushSendQueue();
        }
    }

    class ReliableConnection
    {
        private NetworkConnection _connection;
        private Dictionary<int, MessageState> _sentMessages = new Dictionary<int, MessageState>();
        private Dictionary<int, MessageState> _receivedMessages = new Dictionary<int, MessageState>();
        private UInt16 _sendMessageId;
        public const int FRAGMENT_HEADER_SIZE = sizeof(UInt16) * 3;
        public const int FRAGMENT_DATA_MAX_SIZE = GlobalConfig.MAX_MTU - NetworkConnection.PACKET_HEADER_SIZE_BYTES - NetworkConnection.MESSAGE_HEADER_SIZE_BYTES - FRAGMENT_HEADER_SIZE;

        public event MessageReceived ReliableMessageRecieved;

        public ReliableConnection(NetworkConnection connection)
        {
            _connection = connection;
            _connection.MessageLost += _connection_MessageLost;
            _connection.MessageAcked += _connection_MessageAcked;
            _connection.MessageReceived += _connection_MessageReceived;
        }

        private class MessageState
        {
            public UInt16 id;
            public UInt16 totalFragments;
            public Dictionary<UInt16, byte[]> fragments;
            public IPEndPoint fromAddr;
        }

        public void Send(byte[] message)
        {
            // Reserve a message ID.
            var messageId = _sendMessageId;
            _sendMessageId += 1;

            // Split the message up into fragments.
            var totalFragments = (UInt16)(int)Math.Ceiling((double)message.Length / FRAGMENT_DATA_MAX_SIZE);
            var fragments = new Dictionary<UInt16, byte[]>();
            for (var i = 0; i < totalFragments; i++)
            {
                var offset = i * FRAGMENT_DATA_MAX_SIZE;
                var length = (int)Math.Min(FRAGMENT_DATA_MAX_SIZE, message.Length - offset) + FRAGMENT_HEADER_SIZE;
                fragments[(UInt16)i] = new byte[length];
                Array.Copy(BitConverter.GetBytes((ushort)messageId), 0, fragments[(UInt16)i], 0, sizeof(ushort));
                Array.Copy(BitConverter.GetBytes((ushort)i), 0, fragments[(UInt16)i], sizeof(ushort), sizeof(ushort));
                Array.Copy(BitConverter.GetBytes((ushort)totalFragments), 0, fragments[(UInt16)i], sizeof(ushort) * 2, sizeof(ushort));
                Array.Copy(message, offset, fragments[(UInt16)i], FRAGMENT_HEADER_SIZE, length - FRAGMENT_HEADER_SIZE);
            }

            // Create the message state.
            var state = new MessageState
            {
                id = messageId,
                totalFragments = totalFragments,
                fragments = fragments,
                fromAddr = null,
            };

            // Track and send.
            _sentMessages.Add(_sendMessageId, state);
            foreach (var fragment in fragments)
            {
                _connection.EnqueueSend(fragment.Value);
            }
        }

        private void _connection_MessageLost(IPEndPoint sentToAddr, byte[] message)
        {
            ushort messageId = BitConverter.ToUInt16(message, 0);
            ushort i = BitConverter.ToUInt16(message, sizeof(ushort));
            ushort totalFragments = BitConverter.ToUInt16(message, sizeof(ushort) * 2);

            if (_sentMessages.ContainsKey(messageId))
            {
                // Resend.
                _connection.EnqueueSend(message);
            }
        }

        private void _connection_MessageAcked(IPEndPoint sentToAddr, byte[] message)
        {
            ushort messageId = BitConverter.ToUInt16(message, 0);
            ushort i = BitConverter.ToUInt16(message, sizeof(ushort));
            ushort totalFragments = BitConverter.ToUInt16(message, sizeof(ushort) * 2);

            if (_sentMessages.ContainsKey(messageId))
            {
                // Remove from list of pending fragments.
                _sentMessages[messageId].fragments.Remove(i);

                // If we've sent everything and there's nothing left to be acked, remove the message
                // state on the sender as we don't need to track it any more.
                if (_sentMessages[messageId].fragments.Count == 0)
                {
                    _sentMessages.Remove(messageId);
                }
            }
        }

        private void _connection_MessageReceived(IPEndPoint fromAddr, byte[] message)
        {
            ushort messageId = BitConverter.ToUInt16(message, 0);
            ushort i = BitConverter.ToUInt16(message, sizeof(ushort));
            ushort totalFragments = BitConverter.ToUInt16(message, sizeof(ushort) * 2);

            if (!_receivedMessages.ContainsKey(messageId))
            {
                // New message coming in that we haven't seen before. Start tracking it.
                _receivedMessages.Add(messageId, new MessageState
                {
                    id = messageId,
                    totalFragments = totalFragments,
                    fragments = new Dictionary<ushort, byte[]>(),
                    fromAddr = fromAddr,
                });
            }

            var state = _receivedMessages[messageId];

            // Check to make sure this packet is from the same address.
            if (!fromAddr.Equals(state.fromAddr))
            {
                // Ignore packet from different address.
                return;
            }

            // Store fragment (without the header part, as we don't need it and it makes defragmentation simpler).
            var data = new byte[message.Length - FRAGMENT_HEADER_SIZE];
            Array.Copy(message, FRAGMENT_HEADER_SIZE, data, 0, data.Length);
            state.fragments[i] = data;

            // If we've received all the fragments we need, reconstruct the message
            // and fire the received message event.
            if (state.fragments.Count == state.totalFragments)
            {
                var buffer = new byte[FRAGMENT_DATA_MAX_SIZE * state.totalFragments];
                var length = 0;
                for (var idx = 0; idx < state.totalFragments; idx++)
                {
                    var fragment = state.fragments[(ushort)idx];
                    Array.Copy(fragment, 0, buffer, length, fragment.Length);
                    length += fragment.Length;
                }
                var final = new byte[length];
                Array.Copy(buffer, 0, final, 0, length);
                ReliableMessageRecieved?.Invoke(state.fromAddr, final);
                _receivedMessages.Remove(messageId);
            }
        }
    }

    class GameClient
    {
        private readonly UdpClient _udp;
        private readonly IPEndPoint _target;
        private readonly NetworkConnection _server;
        private readonly ReliableConnection _reliable;

        public event MessageReceived MessageReceived;
        public event MessageReceived ReliableMessageReceived;
        public event ClientDisconnected ServerDisconnected;
        public event MessageAcked MessageAcked;
        public event MessageLost MessageLost;

        public GameClient(string address, int port)
        {
            _udp = new UdpClient(0) { Client = { Blocking = false } };
            _target = new IPEndPoint(IPAddress.Parse(address), port);
            _server = new NetworkConnection(_udp, _target);
            _server.MessageReceived += (fromAddr, message) => MessageReceived?.Invoke(fromAddr, message);
            _server.ClientDisconnected += (fromAddr) => ServerDisconnected?.Invoke(fromAddr);
            _server.MessageAcked += (fromAddr, message) => MessageAcked?.Invoke(fromAddr, message);
            _server.MessageLost += (fromAddr, message) => MessageLost?.Invoke(fromAddr, message);
            _reliable = new ReliableConnection(_server);
            _reliable.ReliableMessageRecieved += (fromAddr, message) => ReliableMessageReceived?.Invoke(fromAddr, message);
        }

        public void Send(byte[] message)
        {
            _server.EnqueueSend(message);
        }

        public void LargeReliableSend(byte[] message)
        {
            _reliable.Send(message);
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
        private readonly Dictionary<IPEndPoint, ReliableConnection> _reliableClients;

        public event MessageReceived MessageReceived;
        public event MessageReceived ReliableMessageReceived;
        public event ClientConnected ClientConnected;
        public event ClientDisconnected ClientDisconnected;
        public event MessageAcked MessageAcked;
        public event MessageLost MessageLost;

        public GameServer(int port)
        {
            _udp = new UdpClient(port) { Client = { Blocking = false } };
            _clients = new Dictionary<IPEndPoint, NetworkConnection>();
            _reliableClients = new Dictionary<IPEndPoint, ReliableConnection>();
        }

        public void Send(IPEndPoint clientAddr, byte[] message)
        {
            if (!_clients.ContainsKey(clientAddr))
            {
                // This isn't a client the server has spoken to before; this kind of
                // server-initiated connection doesn't make a lot of sense, but eh,
                // it's not hard to support.
                HandleNewClientConnection(clientAddr);
            }

            _clients[clientAddr].EnqueueSend(message);
        }

        public void LargeReliableSend(IPEndPoint clientAddr, byte[] message)
        {
            if (!_clients.ContainsKey(clientAddr))
            {
                // This isn't a client the server has spoken to before; this kind of
                // server-initiated connection doesn't make a lot of sense, but eh,
                // it's not hard to support.
                HandleNewClientConnection(clientAddr);
            }

            _reliableClients[clientAddr].Send(message);
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
                    HandleNewClientConnection(receiveAddr);
                }

                _clients[receiveAddr].EnqueueReceive(receiveAddr, packet);
            }

            // Process all pending send operations for connected clients.
            foreach (var kv in _clients.ToArray())
            {
                kv.Value.Update();
            }
        }

        private void HandleNewClientConnection(IPEndPoint clientAddr)
        {
            _clients[clientAddr] = new NetworkConnection(_udp, clientAddr);
            _clients[clientAddr].MessageReceived += (fromAddr, message) => MessageReceived?.Invoke(fromAddr, message);
            _clients[clientAddr].ClientDisconnected += (fromAddr) =>
            {
                _clients.Remove(fromAddr);
                _reliableClients.Remove(fromAddr);
                ClientDisconnected?.Invoke(fromAddr);
            };
            _clients[clientAddr].MessageAcked += (fromAddr, message) => MessageAcked?.Invoke(fromAddr, message);
            _clients[clientAddr].MessageLost += (fromAddr, message) => MessageLost?.Invoke(fromAddr, message);
            _reliableClients[clientAddr] = new ReliableConnection(_clients[clientAddr]);
            _reliableClients[clientAddr].ReliableMessageRecieved += (fromAddr, message) => ReliableMessageReceived?.Invoke(fromAddr, message);
            ClientConnected?.Invoke(clientAddr);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var reallyBigMessage = "";
            for (var i = 0; i < 1024; i++)
            {
                reallyBigMessage += "THIS_IS_A_REALLY_BIG_MESSAGE_";
            }

            if (args.Length >= 1 && args[0] == "server")
            {
                GameServer server = null;
                server = new GameServer(19000);
                server.ReliableMessageReceived += (from, data) => {
                    // Server just verified message from client.
                    var message = Encoding.ASCII.GetString(data);
                    if (message == reallyBigMessage)
                    {
                        Console.WriteLine("[server] Got correct message - " + from + " - " + message.Length + " bytes");
                    }
                    else
                    {
                        Console.WriteLine("[server] Got MISMATCHED message - " + from + " - " + message.Length + " bytes");
                    }
                };
                server.ClientConnected += (addr) => {
                    Console.WriteLine("[server] Client connected - " + addr);
                };
                server.ClientDisconnected += (addr) => {
                    Console.WriteLine("[server] Client disconnected - " + addr);
                };

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
                var client = new GameClient("127.0.0.1", 19000);
                client.ServerDisconnected += (addr) => {
                    Console.WriteLine("[client] Lost connection to server");
                    running = false;
                };

                Console.WriteLine("[client] Started");

                var i = 0;
                while (running)
                {
                    if (i++ % 100 == 0)
                    {
                        // Enqueue send.
                        Console.WriteLine("[client] Sent large message to server - " + reallyBigMessage.Length + " bytes");
                        client.LargeReliableSend(Encoding.ASCII.GetBytes(reallyBigMessage));
                    }

                    // Update client.
                    client.Update();

                    // Tick.
                    Thread.Sleep(1000 / GlobalConfig.TICK_RATE);
                }
            }
        }
    }
}
