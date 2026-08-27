#!/usr/bin/env python3

# Test that a malformed bridge block in the config file does not crash the
# broker on reload, and leaves the running bridge untouched. Partially parsed
# bridge configs are freed on reload, so every parser error path must leave
# the bridge struct in a freeable state.

from mosq_test_helper import *
import signal
import traceback

def write_config(filename, port1, port2, extra_lines):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port2))
        f.write("allow_anonymous true\n")
        f.write("\n")
        f.write("connection bridge-a\n")
        f.write("remote_clientid bridge-a\n")
        f.write("address 127.0.0.1:%d\n" % (port1))
        f.write("topic a/# out\n")
        f.write("cleansession true\n")
        f.write("notifications false\n")
        f.write("restart_timeout 5\n")
        f.write("try_private false\n")
        f.write("bridge_protocol_version mqttv311\n")
        for line in extra_lines:
            f.write(line + "\n")


def accept_bridge(sock):
    (conn, address) = sock.accept()
    conn.settimeout(20)
    connect_packet = mosq_test.gen_connect("bridge-a", keepalive=60, proto_ver=4)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=4)
    unsubscribe_packet = mosq_test.gen_unsubscribe(1, "a/#", proto_ver=4)
    unsuback_packet = mosq_test.gen_unsuback(1, proto_ver=4)
    mosq_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)
    mosq_test.expect_packet(conn, "unsubscribe", unsubscribe_packet)
    conn.send(unsuback_packet)
    return conn


BAD_BLOCKS = [
    # Invalid port in the second address: parsed after address_count was bumped
    ["", "connection bridge-bad", "address 127.0.0.1:1883 127.0.0.2:0", "topic b/# out"],
    # Invalid prefix+topic combination: rejected inside bridge__add_topic()
    # after topic_count was bumped and the topic string allocated
    ["", "connection bridge-bad", "address 127.0.0.1:1883", "topic a/#/b out 0 local/ remote/"],
    # No addresses at all
    ["", "connection bridge-bad", "topic b/# out"],
]


def do_test():
    (port1, port2) = mosq_test.get_port(2)
    conf_file = os.path.basename(__file__).replace('.py', '.conf')
    write_config(conf_file, port1, port2, [])

    rc = 1

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(10)
    sock.bind(('', port1))
    sock.listen(5)

    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port2, use_conf=True)

    conn = None
    try:
        conn = accept_bridge(sock)

        for bad in BAD_BLOCKS:
            write_config(conf_file, port1, port2, bad)
            broker.send_signal(signal.SIGHUP)
            time.sleep(0.5)
            if broker.poll() is not None:
                raise mosq_test.TestError("Broker died on reload with malformed bridge block: %r" % (bad,))
            # Running bridge untouched, broker responsive
            mosq_test.do_ping(conn)
            connect_packet = mosq_test.gen_connect("probe", keepalive=60)
            connack_packet = mosq_test.gen_connack(rc=0)
            probe = mosq_test.do_client_connect(connect_packet, connack_packet, port=port2, timeout=10)
            probe.close()

        rc = 0
    except mosq_test.TestError as e:
        print("FAIL: %s" % (e,))
    except Exception:
        traceback.print_exc()
    finally:
        os.remove(conf_file)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if conn: conn.close()
        sock.close()
        if rc == 0:
            log = stde.decode('utf-8')
            if log.count("Error reloading config") != len(BAD_BLOCKS):
                print("FAIL: expected %d reload errors, log:\n%s" % (len(BAD_BLOCKS), log))
                rc = 1
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

do_test()
exit(0)
