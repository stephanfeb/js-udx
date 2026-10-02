/// dart-udx peer for js-udx interop tests. Same CLI and stderr protocol as
/// tools/go-peer:
///
///   peer listen [host]                      echo every stream; prints "READY <port>" on stderr
///   peer dial <host:port> <bytes> <streams> send a pattern on each stream, verify the echo;
///                                           prints "RESULT <bytes>" or "CORRUPT <detail>"
///
/// The payload pattern is go-udx's interop pattern, (i*31 + i~/251) & 0xff with
/// i = byte index + stream index * 1000.
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_udx/dart_udx.dart';

Uint8List pattern(int n, int seed) {
  final b = Uint8List(n);
  for (var i = 0; i < n; i++) {
    final j = i + seed;
    b[i] = (j * 31 + j ~/ 251) & 0xff;
  }
  return b;
}

Never fail(String message) {
  stderr.writeln(message);
  exit(1);
}

/// Writes everything [s] receives back to it, then ends its side.
Future<void> echo(UDXStream s) async {
  try {
    await for (final chunk in s.data) {
      await s.add(chunk);
    }
    await s.closeWrite();
  } catch (e) {
    stderr.writeln('ECHO_ERROR stream ${s.id}: $e');
  }
}

Future<void> listen(String host) async {
  final raw = await RawDatagramSocket.bind(host, 0);
  final mux = UDXMultiplexer(raw);
  stderr.writeln('READY ${raw.port}');
  mux.connections.listen((UDPSocket socket) {
    // A stream that arrived before this listener is buffered and re-emitted by
    // flushStreamBuffer; one that arrived after can be emitted again by a later
    // flush. Echo each exactly once.
    final seen = <UDXStream>{};
    socket.on('stream').listen((e) {
      final s = e.data as UDXStream;
      if (seen.add(s)) unawaited(echo(s));
    });
    socket.flushStreamBuffer();
  });
}

Future<void> dial(String addr, int size, int streams) async {
  final i = addr.lastIndexOf(':');
  final host = addr.substring(0, i);
  final port = int.parse(addr.substring(i + 1));

  final raw = await RawDatagramSocket.bind(InternetAddress.anyIPv4, 0);
  final mux = UDXMultiplexer(raw);
  final socket = mux.createSocket(UDX(), host, port);

  // Like dart-libp2p and bulk_peer: the initiator takes odd local stream ids.
  final opened = <UDXStream>[];
  for (var k = 0; k < streams; k++) {
    opened.add(await UDXStream.createOutgoing(UDX(), socket, 1 + 2 * k, 0, host, port));
  }
  await socket.handshakeComplete.timeout(const Duration(seconds: 10));

  final errors = <String>[];
  await Future.wait(List.generate(streams, (k) async {
    final s = opened[k];
    final want = pattern(size, k * 1000);
    final got = BytesBuilder(copy: false);
    final reading = s.data.forEach(got.add);
    const chunk = 32 * 1024;
    for (var o = 0; o < size; o += chunk) {
      await s.add(Uint8List.sublistView(want, o, o + chunk < size ? o + chunk : size));
    }
    await s.closeWrite();
    try {
      await reading.timeout(const Duration(seconds: 60));
    } catch (e) {
      errors.add('stream $k read: $e after ${got.length} bytes');
      return;
    }
    final bytes = got.takeBytes();
    var same = bytes.length == want.length;
    for (var j = 0; same && j < bytes.length; j++) {
      same = bytes[j] == want[j];
    }
    if (!same) errors.add('stream $k: got ${bytes.length} bytes, want ${want.length} (or content differs)');
  })).timeout(const Duration(seconds: 90), onTimeout: () => fail('CORRUPT timed out'));

  if (errors.isNotEmpty) fail('CORRUPT ${errors.join('; ')}');
  stderr.writeln('RESULT ${size * streams}');
  await socket.close();
  mux.close();
  exit(0);
}

Future<void> main(List<String> args) async {
  if (args.isEmpty) fail('usage: peer listen [host] | dial <addr> <bytes> <streams>');
  switch (args[0]) {
    case 'listen':
      await listen(args.length > 1 ? args[1] : '127.0.0.1');
    case 'dial':
      if (args.length != 4) fail('usage: peer dial <addr> <bytes> <streams>');
      await dial(args[1], int.parse(args[2]), int.parse(args[3]));
    default:
      fail('unknown mode ${args[0]}');
  }
}
