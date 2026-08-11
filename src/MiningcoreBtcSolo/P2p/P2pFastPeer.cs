using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using MiningcoreBtcSolo.Template;
using MiningcoreBtcSolo.Util;

namespace MiningcoreBtcSolo.P2p;

/// <summary>
/// Minimal Bitcoin P2P client for early headers/cmpctblock announcements.
/// </summary>
public sealed class P2pFastPeer
{
    private readonly AppConfig _cfg;
    private readonly TemplateEngine _engine;
    private const int ProtocolVersion = 70016;
    private const uint MsgBlock = 2;
    private const uint MsgCmpctBlock = 4;
    private const uint MsgWitnessFlag = 1u << 30;
    private const ulong ServiceNetwork = 1;
    private const ulong ServiceWitness = 1UL << 3;
    private const ulong ServiceNetworkLimited = 1UL << 10;

    public P2pFastPeer(AppConfig cfg, TemplateEngine engine)
    {
        _cfg = cfg;
        _engine = engine;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var peer = _cfg.Bitcoind.P2pFastPeer;
        if (string.IsNullOrWhiteSpace(peer))
            return;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunSessionAsync(peer.Trim(), ct);
            }
            catch (Exception ex)
            {
                SoloLog.Warn("p2p fast peer session failed", ("peer", peer), ("error", ex.Message));
            }
            await Task.Delay(3000, ct);
        }
    }

    private async Task RunSessionAsync(string peer, CancellationToken ct)
    {
        var (host, port) = ParsePeer(peer);
        using var client = new TcpClient();
        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        connectCts.CancelAfter(TimeSpan.FromSeconds(5));
        await client.ConnectAsync(host, port, connectCts.Token);
        client.NoDelay = true;
        await using var stream = client.GetStream();

        var remote = (IPEndPoint)client.Client.RemoteEndPoint!;
        // Advertise a tip height so the peer treats us as caught-up (cmpct high-bandwidth).
        var startHeight = (int)Math.Max(0, (long)_engine.ActiveMiningJob.Height - 1);
        await SendMessageAsync(stream, "version", BuildVersionPayload(remote, startHeight), ct);

        var sentVerack = false;
        var receivedVerack = false;

        while (!ct.IsCancellationRequested)
        {
            var msg = await ReadMessageAsync(stream, ct);
            switch (msg.Command)
            {
                case "version":
                    if (!sentVerack)
                    {
                        await SendMessageAsync(stream, "verack", Array.Empty<byte>(), ct);
                        sentVerack = true;
                    }
                    break;
                case "verack":
                    receivedVerack = true;
                    await SendMessageAsync(stream, "sendheaders", Array.Empty<byte>(), ct);
                    await SendMessageAsync(stream, "sendcmpct", BuildSendCmpct(2), ct);
                    await SendMessageAsync(stream, "sendcmpct", BuildSendCmpct(1), ct);
                    SoloLog.Info("p2p fast peer ready",
                        ("peer", $"{host}:{port}"),
                        ("start_height", startHeight));
                    break;
                case "ping":
                    if (msg.Payload.Length >= 8)
                        await SendMessageAsync(stream, "pong", msg.Payload.AsSpan(0, 8).ToArray(), ct);
                    break;
                case "headers" when receivedVerack:
                    await HandleHeadersAsync(msg.Payload, ct);
                    break;
                case "cmpctblock" when receivedVerack:
                    await HandleCmpctBlockAsync(msg.Payload, ct);
                    break;
                case "inv" when receivedVerack:
                    await HandleInvAsync(stream, msg.Payload, ct);
                    break;
                case "block" when receivedVerack:
                    // Full block — use header only for empty-fast tip switch.
                    if (msg.Payload.Length >= 80)
                        await HandleHeaderBytesAsync(msg.Payload, ct);
                    break;
            }
        }
    }

    private async Task HandleInvAsync(NetworkStream stream, byte[] payload, CancellationToken ct)
    {
        var pos = 0;
        if (!ReadVarInt(payload, ref pos, out var count) || count == 0 || count > 50_000)
            return;

        var hasBlock = false;
        for (ulong i = 0; i < count; i++)
        {
            if (pos + 36 > payload.Length)
                return;
            var type = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(pos, 4));
            pos += 4;
            pos += 32; // hash (LE)

            // MSG_BLOCK / MSG_WITNESS_BLOCK / MSG_CMPCT_BLOCK
            var baseType = type & ~MsgWitnessFlag;
            if (baseType is MsgBlock or MsgCmpctBlock)
                hasBlock = true;
        }

        if (!hasBlock)
            return;

        // Solo: headers-first — request headers from our tip, not getdata/cmpctblock body.
        var job = _engine.ActiveMiningJob;
        if (!job.Ready || string.IsNullOrEmpty(job.PrevhashBe))
            return;

        byte[] tipLe;
        try
        {
            tipLe = Hex.ReverseCopy(Hex.Decode(job.PrevhashBe));
        }
        catch
        {
            return;
        }

        using var outMs = new MemoryStream();
        using (var w = new BinaryWriter(outMs))
        {
            w.Write(ProtocolVersion);
            BitcoinEncoding.WriteVarInt(w, 1UL);
            w.Write(tipLe);
            w.Write(new byte[32]); // hash_stop = zero → all headers after locator
        }

        SoloLog.Info("p2p inv → getheaders", ("locator", job.PrevhashBe));
        await SendMessageAsync(stream, "getheaders", outMs.ToArray(), ct);
    }

    private async Task HandleHeadersAsync(byte[] payload, CancellationToken ct)
    {
        if (!TryParseHeadersPayload(payload, out var announcements))
            throw new InvalidDataException("malformed p2p headers payload or count exceeds 2000");

        var job = _engine.ActiveMiningJob;
        if (!job.Ready)
            return;

        for (var i = 0; i < announcements.Length; i++)
        {
            var annHeightValue = (ulong)job.Height + (uint)i;
            if (annHeightValue > uint.MaxValue)
                return;

            var ann = announcements[i];
            // Announced block height ≈ current job height (the block we were mining) + i
            var annHeight = (uint)annHeightValue;
            await _engine.HandleP2pFastAnnouncementAsync(
                ann.PrevhashHex,
                ann.BlockHashHex,
                ann.BlockTime,
                annHeight,
                ann.BlockNbits,
                ann.BlockVersion,
                ct);
        }
    }

    private async Task HandleCmpctBlockAsync(byte[] payload, CancellationToken ct)
    {
        if (payload.Length < 80)
            return;
        await HandleHeaderBytesAsync(payload, ct);
    }

    private async Task HandleHeaderBytesAsync(byte[] header80, CancellationToken ct)
    {
        if (header80.Length != 80)
            return;

        var job = _engine.ActiveMiningJob;
        if (!job.Ready)
            return;
        var ann = ParseHeader(header80);
        var annHeight = job.Height;
        await _engine.HandleP2pFastAnnouncementAsync(
            ann.PrevhashHex,
            ann.BlockHashHex,
            ann.BlockTime,
            annHeight,
            ann.BlockNbits,
            ann.BlockVersion,
            ct);
    }

    internal static bool TryParseHeadersPayload(
        ReadOnlySpan<byte> payload,
        out HeaderAnnouncement[] announcements)
    {
        announcements = Array.Empty<HeaderAnnouncement>();
        var pos = 0;
        if (!ReadVarInt(payload, ref pos, out var count) || count > 2000)
            return false;

        var parsed = new HeaderAnnouncement[(int)count];
        for (var i = 0; i < parsed.Length; i++)
        {
            if (pos > payload.Length - 80)
                return false;
            var announcement = ParseHeader(payload.Slice(pos, 80));
            pos += 80;
            if (!ReadVarInt(payload, ref pos, out var txnCount) || txnCount != 0)
                return false;
            if (i > 0 && !announcement.PrevhashHex.Equals(
                    parsed[i - 1].BlockHashHex, StringComparison.OrdinalIgnoreCase))
                return false;
            parsed[i] = announcement;
        }

        if (pos != payload.Length)
            return false;
        announcements = parsed;
        return true;
    }

    private static HeaderAnnouncement ParseHeader(ReadOnlySpan<byte> header)
    {
        if (header.Length != 80)
            throw new ArgumentException("block header must be exactly 80 bytes", nameof(header));

        var version = BinaryPrimitives.ReadUInt32LittleEndian(header[..4]);
        var prevLe = header.Slice(4, 32).ToArray();
        var prevBe = Hex.ReverseCopy(prevLe);
        var hashLe = BitcoinEncoding.DoubleSha256(header);
        var hashBe = Hex.ReverseCopy(hashLe);
        var time = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(68, 4));
        var nbits = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(72, 4));
        return new HeaderAnnouncement(
            Hex.Encode(prevBe), Hex.Encode(hashBe), version, time, nbits);
    }

    private (string host, int port) ParsePeer(string peer)
    {
        var defaultPort = _cfg.NetworkName.ToLowerInvariant() switch
        {
            "mainnet" or "bitcoin" => 8333,
            "testnet" => 18333,
            "regtest" => 18444,
            _ => throw new InvalidOperationException($"unsupported P2P network: {_cfg.NetworkName}")
        };
        if (peer.Contains(':'))
        {
            var parts = peer.Split(':');
            return (parts[0], int.Parse(parts[1]));
        }
        return (peer, defaultPort);
    }

    private byte[] BuildVersionPayload(IPEndPoint remote, int startHeight)
    {
        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write(ProtocolVersion);
        w.Write(ServiceNetwork | ServiceWitness | ServiceNetworkLimited);
        w.Write(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        WriteNetAddr(w, remote);
        WriteNetAddr(w, new IPEndPoint(IPAddress.Loopback, 0));
        w.Write((long)(uint)RandomNumberGenerator.GetInt32(int.MaxValue) | ((long)(uint)RandomNumberGenerator.GetInt32(int.MaxValue) << 32));
        WriteVarString(w, AppInfo.P2pUserAgent);
        w.Write(startHeight);
        w.Write((byte)1); // relay
        return ms.ToArray();
    }

    private static void WriteNetAddr(BinaryWriter w, IPEndPoint ep)
    {
        w.Write(ServiceNetwork | ServiceWitness);
        var ip = ep.Address.MapToIPv6().GetAddressBytes();
        w.Write(ip);
        // port big-endian
        w.Write((byte)(ep.Port >> 8));
        w.Write((byte)(ep.Port & 0xff));
    }

    private static void WriteVarString(BinaryWriter w, string s)
    {
        var bytes = Encoding.ASCII.GetBytes(s);
        BitcoinEncoding.WriteVarInt(w, (ulong)bytes.Length);
        w.Write(bytes);
    }

    private static byte[] BuildSendCmpct(ulong version)
    {
        var buf = new byte[9];
        buf[0] = 1; // announce
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(1), version);
        return buf;
    }

    private async Task SendMessageAsync(NetworkStream stream, string command, byte[] payload, CancellationToken ct)
    {
        var magic = NetworkMagic();
        var header = new byte[24];
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(0, 4), magic);
        var cmdBytes = Encoding.ASCII.GetBytes(command);
        Array.Copy(cmdBytes, 0, header, 4, Math.Min(12, cmdBytes.Length));
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(16, 4), (uint)payload.Length);
        var checksum = BitcoinEncoding.DoubleSha256(payload);
        Array.Copy(checksum, 0, header, 20, 4);
        await stream.WriteAsync(header, ct);
        if (payload.Length > 0)
            await stream.WriteAsync(payload, ct);
    }

    internal async Task<(string Command, byte[] Payload)> ReadMessageAsync(NetworkStream stream, CancellationToken ct)
    {
        var header = await ReadExactAsync(stream, 24, ct);
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
        if (magic != NetworkMagic())
            throw new InvalidOperationException("bad p2p magic");
        var cmd = Encoding.ASCII.GetString(header, 4, 12).TrimEnd('\0');
        var len = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
        if (len > 4 * 1024 * 1024)
            throw new InvalidOperationException("p2p payload too large");
        byte[] payload;
        byte[] checksum;
        if (len == 0)
        {
            payload = Array.Empty<byte>();
            checksum = BitcoinEncoding.DoubleSha256(payload);
        }
        else if (cmd is "block" or "cmpctblock")
        {
            // Fast-tip handling needs only the serialized 80-byte block header.
            // Consume the body with a reusable scratch buffer instead of allocating
            // a managed array as large as the announced block.
            (payload, checksum) = await ReadPrefixAndDiscardAsync(stream, (int)len, 80, ct);
        }
        else
        {
            payload = await ReadExactAsync(stream, (int)len, ct);
            checksum = BitcoinEncoding.DoubleSha256(payload);
        }
        if (!CryptographicOperations.FixedTimeEquals(
                checksum.AsSpan(0, 4), header.AsSpan(20, 4)))
            throw new InvalidDataException("bad p2p payload checksum");
        return (cmd, payload);
    }

    private static async Task<(byte[] Prefix, byte[] Checksum)> ReadPrefixAndDiscardAsync(
        NetworkStream stream,
        int totalLength,
        int prefixLength,
        CancellationToken ct)
    {
        var keepLength = Math.Min(totalLength, prefixLength);
        var prefix = keepLength == 0 ? Array.Empty<byte>() : await ReadExactAsync(stream, keepLength, ct);
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        if (prefix.Length > 0)
            hasher.AppendData(prefix);
        var remaining = totalLength - keepLength;
        if (remaining == 0)
        {
            var firstHash = hasher.GetHashAndReset();
            return (prefix, SHA256.HashData(firstHash));
        }

        var scratch = ArrayPool<byte>.Shared.Rent(Math.Min(64 * 1024, remaining));
        try
        {
            while (remaining > 0)
            {
                var n = await stream.ReadAsync(scratch.AsMemory(0, Math.Min(scratch.Length, remaining)), ct);
                if (n == 0)
                    throw new EndOfStreamException();
                hasher.AppendData(scratch, 0, n);
                remaining -= n;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(scratch);
        }
        var payloadHash = hasher.GetHashAndReset();
        return (prefix, SHA256.HashData(payloadHash));
    }

    private static async Task<byte[]> ReadExactAsync(NetworkStream stream, int len, CancellationToken ct)
    {
        var buf = new byte[len];
        var off = 0;
        while (off < len)
        {
            var n = await stream.ReadAsync(buf.AsMemory(off, len - off), ct);
            if (n == 0)
                throw new EndOfStreamException();
            off += n;
        }
        return buf;
    }

    private uint NetworkMagic() => _cfg.NetworkName.ToLowerInvariant() switch
    {
        "mainnet" or "bitcoin" => 0xd9b4bef9,
        "testnet" => 0x0709110b,
        "regtest" => 0xdab5bffa,
        _ => throw new InvalidOperationException($"unsupported P2P network: {_cfg.NetworkName}")
    };

    private static bool ReadVarInt(ReadOnlySpan<byte> data, ref int pos, out ulong value)
    {
        value = 0;
        if (pos >= data.Length) return false;
        var prefix = data[pos++];
        if (prefix < 0xfd)
        {
            value = prefix;
            return true;
        }
        if (prefix == 0xfd)
        {
            if (pos + 2 > data.Length) return false;
            value = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos, 2));
            pos += 2;
            return value >= 0xfd;
        }
        if (prefix == 0xfe)
        {
            if (pos + 4 > data.Length) return false;
            value = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos, 4));
            pos += 4;
            return value > ushort.MaxValue;
        }
        if (pos + 8 > data.Length) return false;
        value = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos, 8));
        pos += 8;
        return value > uint.MaxValue;
    }

    internal readonly record struct HeaderAnnouncement(
        string PrevhashHex,
        string BlockHashHex,
        uint BlockVersion,
        uint BlockTime,
        uint BlockNbits);
}
