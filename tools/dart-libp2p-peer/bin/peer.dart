/// dart-libp2p host over UDX for js-udx's libp2p interop tests. Same CLI and
/// stderr protocol as tools/go-libp2p-peer:
///
///   peer listen                   echo /echo/1.0.0 streams; prints "READY <multiaddr>/p2p/<id>" on stderr
///   peer dial <multiaddr> <bytes> connect, ping, identify, echo <bytes>; prints "PING <ms>",
///                                 "PROTOCOLS <ids…>", "AGENT <version>", then "RESULT <bytes>"
///                                 or "CORRUPT <detail>" on stderr. It reads exactly <bytes>
///                                 back, not to EOF.
///
/// The payload pattern is go-udx's interop pattern, (i*31 + i~/251) & 0xff.
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_libp2p/config/config.dart' as p2p_config;
import 'package:dart_libp2p/core/crypto/ed25519.dart' as crypto_ed25519;
import 'package:dart_libp2p/core/host/host.dart';
import 'package:dart_libp2p/core/multiaddr.dart';
import 'package:dart_libp2p/core/network/context.dart' as core_context;
import 'package:dart_libp2p/core/network/stream.dart';
import 'package:dart_libp2p/core/peer/peer_id.dart';
import 'package:dart_libp2p/p2p/multiaddr/protocol.dart';
import 'package:dart_libp2p/p2p/protocol/ping/ping.dart';
import 'package:dart_libp2p/p2p/security/noise/noise_protocol.dart';
import 'package:dart_libp2p/p2p/transport/connection_manager.dart' as p2p_conn_manager;
import 'package:dart_libp2p/p2p/transport/udx_transport.dart';
import 'package:dart_udx/dart_udx.dart';

const echoProtocol = '/echo/1.0.0';

Never fail(String message) {
  stderr.writeln(message);
  exit(1);
}

Uint8List pattern(int n) {
  final b = Uint8List(n);
  for (var i = 0; i < n; i++) {
    b[i] = (i * 31 + i ~/ 251) & 0xff;
  }
  return b;
}

Future<Host> newHost({String? listen}) async {
  final keyPair = await crypto_ed25519.generateEd25519KeyPair();
  final connManager = p2p_conn_manager.ConnectionManager();
  return await p2p_config.Libp2p.new_([
    p2p_config.Libp2p.identity(keyPair),
    p2p_config.Libp2p.connManager(connManager),
    p2p_config.Libp2p.transport(UDXTransport(connManager: connManager, udxInstance: UDX())),
    p2p_config.Libp2p.security(await NoiseSecurity.create(keyPair)),
    if (listen != null) p2p_config.Libp2p.listenAddrs([MultiAddr(listen)]),
    // Keep loopback addresses: the tests run on one machine.
    p2p_config.Libp2p.addrsFactory((addrs) => addrs),
  ]);
}

/// Reads [n] bytes, or until the stream ends. Not to EOF: go-yamux and
/// js-libp2p send their FIN on a WINDOW_UPDATE, which a dart-libp2p without
/// that fix doesn't see (jsudx-aof).
Future<Uint8List> readN(P2PStream stream, int n) async {
  final out = BytesBuilder(copy: false);
  while (out.length < n) {
    final chunk = await stream.read();
    if (chunk.isEmpty) break;
    out.add(chunk);
  }
  return out.takeBytes();
}

Future<void> listen() async {
  final host = await newHost(listen: '/ip4/127.0.0.1/udp/0/udx');
  await host.start();
  host.setStreamHandler(echoProtocol, (stream, remotePeer) async {
    try {
      while (true) {
        final chunk = await stream.read();
        if (chunk.isEmpty) break;
        await stream.write(chunk);
      }
      await stream.closeWrite();
    } catch (e) {
      stderr.writeln('ECHO_ERROR $e');
      await stream.reset();
    }
  });
  final addr = host.addrs.firstWhere((a) => a.toString().contains('/udx'),
      orElse: () => fail('not listening on UDX: ${host.addrs}'));
  stderr.writeln('READY $addr/p2p/${host.id}');
  await Completer<void>().future;
}

Future<void> dial(String target, int size) async {
  final ma = MultiAddr(target);
  final idStr = ma.valueForProtocol(Protocols.p2p.name) ?? fail('target needs /p2p/<id>');
  final peerId = PeerId.fromString(idStr);
  final host = await newHost();
  await host.start();
  await host.peerStore.addrBook.addAddrs(peerId, [ma.decapsulate(Protocols.p2p.name)!], const Duration(hours: 1));

  final ping = await PingService(host).ping(peerId).first.timeout(const Duration(seconds: 30));
  if (ping.hasError) fail('ping: ${ping.error}');
  stderr.writeln('PING ${ping.rtt!.inMilliseconds}');

  // Identify runs on connect; give it a moment to record the peer.
  var protocols = <String>[];
  for (var i = 0; i < 50 && protocols.isEmpty; i++) {
    protocols = (await host.peerStore.protoBook.getProtocols(peerId)).map((p) => p.toString()).toList();
    if (protocols.isEmpty) await Future.delayed(const Duration(milliseconds: 100));
  }
  stderr.writeln('PROTOCOLS ${protocols.join(' ')}');
  stderr.writeln('AGENT ${await host.peerStore.peerMetadata.get(peerId, 'AgentVersion')}');

  final stream = await host
      .newStream(peerId, [echoProtocol], core_context.Context())
      .timeout(const Duration(seconds: 15));
  final want = pattern(size);
  final reading = readN(stream, size);
  const chunk = 64 * 1024;
  for (var o = 0; o < size; o += chunk) {
    await stream.write(Uint8List.sublistView(want, o, o + chunk < size ? o + chunk : size));
  }
  await stream.closeWrite();
  final got = await reading.timeout(const Duration(seconds: 60));
  var same = got.length == want.length;
  for (var i = 0; same && i < got.length; i++) {
    same = got[i] == want[i];
  }
  if (!same) fail('CORRUPT got ${got.length} bytes, want ${want.length} (or content differs)');
  stderr.writeln('RESULT ${got.length}');
  await host.close();
  exit(0);
}

Future<void> main(List<String> args) async {
  if (args.isEmpty) fail('usage: peer listen | dial <multiaddr> <bytes>');
  switch (args[0]) {
    case 'listen':
      await listen();
    case 'dial':
      if (args.length != 3) fail('usage: peer dial <multiaddr> <bytes>');
      await dial(args[1], int.parse(args[2]));
    default:
      fail('unknown mode ${args[0]}');
  }
}
