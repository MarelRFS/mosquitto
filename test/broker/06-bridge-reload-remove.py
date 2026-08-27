#!/usr/bin/env python3

# Test that on SIGHUP:
#  - a bridge removed from the config file is stopped and its retained
#    $SYS/broker/connection/<id>/state message is cleared,
#  - a bridge whose options changed is restarted with the new options,
#  - an unchanged config does not restart running bridges,
#  - a bridge added back is started again.

from mosq_test_helper import *
import signal
import traceback

def write_config(filename, port1, port2, bridges):
    # bridges: list of (name, topic)
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port2))
        f.write("allow_anonymous true\n")
        for (name, topic) in bridges:
            f.write("\n")
            f.write("connection %s\n" % (name))
            f.write("remote_clientid %s\n" % (name))
            f.write("address 127.0.0.1:%d\n" % (port1))
            f.write("topic %s/# out\n" % (topic))
            f.write("cleansession true\n")
            f.write("notifications true\n")
            f.write("notifications_local_only true\n")
            f.write("restart_timeout 5\n")
            f.write("try_private false\n")
            f.write("bridge_protocol_version mqttv311\n")


def state_topic(name):
    return "$SYS/broker/connection/%s/state" % (name)


def accept_bridge(sock, name, topic):
    (conn, address) = sock.accept()
    conn.settimeout(20)

    connect_packet = mosq_test.gen_connect(name, keepalive=60, proto_ver=4)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=4)
    unsubscribe_packet = mosq_test.gen_unsubscribe(1, "%s/#" % (topic), proto_ver=4)
    unsuback_packet = mosq_test.gen_unsuback(1, proto_ver=4)

    mosq_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)
    mosq_test.expect_packet(conn, "unsubscribe", unsubscribe_packet)
    conn.send(unsuback_packet)
    return conn


def check_forward(local_port, conn, name, topic):
    payload = "reload-%s" % (name)
    publish_packet = mosq_test.gen_publish("%s/msg" % (topic), qos=0, payload=payload)

    connect_packet = mosq_test.gen_connect("pub-%s" % (name), keepalive=60)
    connack_packet = mosq_test.gen_connack(rc=0)
    lsock = mosq_test.do_client_connect(connect_packet, connack_packet, port=local_port, timeout=10)
    lsock.send(publish_packet)
    lsock.close()

    mosq_test.expect_packet(conn, "forwarded publish", publish_packet)


def expect_no_connection(sock):
    sock.settimeout(2)
    try:
        (conn, address) = sock.accept()
        conn.close()
        raise mosq_test.TestError("Unexpected bridge connection")
    except socket.timeout:
        pass
    finally:
        sock.settimeout(10)


def expect_closed(conn, what):
    conn.settimeout(5)
    try:
        data = conn.recv(1)
    except socket.timeout:
        raise mosq_test.TestError("%s: expected connection to be closed" % (what))
    if len(data) != 0:
        raise mosq_test.TestError("%s: expected EOF, got data" % (what))
    conn.close()


def check_retained_state(local_port, name, expected):
    # expected: "1" for a retained state message, or None for no retained message.
    connect_packet = mosq_test.gen_connect("sub-%s" % (name), keepalive=60)
    connack_packet = mosq_test.gen_connack(rc=0)
    subscribe_packet = mosq_test.gen_subscribe(5, state_topic(name), 0)
    suback_packet = mosq_test.gen_suback(5, 0)
    lsock = mosq_test.do_client_connect(connect_packet, connack_packet, port=local_port, timeout=10)
    mosq_test.do_send_receive(lsock, subscribe_packet, suback_packet, "suback")
    if expected is None:
        lsock.settimeout(1)
        try:
            data = lsock.recv(100)
            raise mosq_test.TestError("Unexpected retained state message for %s: %r" % (name, data))
        except socket.timeout:
            pass
    else:
        publish_packet = mosq_test.gen_publish(state_topic(name), qos=0, payload=expected, retain=True)
        mosq_test.expect_packet(lsock, "retained state", publish_packet)
    lsock.close()


def do_test():
    (port1, port2) = mosq_test.get_port(2)
    conf_file = os.path.basename(__file__).replace('.py', '.conf')
    rc = 1

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(10)
    sock.bind(('', port1))
    sock.listen(5)

    # Start with a single bridge so accept order is deterministic, then add
    # the second one via reload (covered by 06-bridge-reload-add.py).
    write_config(conf_file, port1, port2, [("bridge-a", "topic-a")])
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port2, use_conf=True)

    conn_a = None
    conn_b = None
    try:
        conn_a = accept_bridge(sock, "bridge-a", "topic-a")
        check_forward(port2, conn_a, "bridge-a", "topic-a")

        write_config(conf_file, port1, port2, [("bridge-a", "topic-a"), ("bridge-b", "topic-b")])
        broker.send_signal(signal.SIGHUP)
        conn_b = accept_bridge(sock, "bridge-b", "topic-b")
        check_forward(port2, conn_b, "bridge-b", "topic-b")
        check_retained_state(port2, "bridge-a", "1")
        check_retained_state(port2, "bridge-b", "1")

        # 1. Unchanged config: nothing restarts
        broker.send_signal(signal.SIGHUP)
        expect_no_connection(sock)
        mosq_test.do_ping(conn_a)
        mosq_test.do_ping(conn_b)

        # 2. Remove bridge-b
        write_config(conf_file, port1, port2, [("bridge-a", "topic-a")])
        broker.send_signal(signal.SIGHUP)
        expect_closed(conn_b, "bridge-b removed")
        conn_b = None
        expect_no_connection(sock)
        mosq_test.do_ping(conn_a)
        check_retained_state(port2, "bridge-b", None)
        check_retained_state(port2, "bridge-a", "1")

        # 3. Change bridge-a's topic: restarted with new topic
        write_config(conf_file, port1, port2, [("bridge-a", "topic-a2")])
        broker.send_signal(signal.SIGHUP)
        expect_closed(conn_a, "bridge-a changed")
        conn_a = accept_bridge(sock, "bridge-a", "topic-a2")
        check_forward(port2, conn_a, "bridge-a", "topic-a2")
        check_retained_state(port2, "bridge-a", "1")

        # 4. Unchanged again after a restart: still stable
        broker.send_signal(signal.SIGHUP)
        expect_no_connection(sock)
        mosq_test.do_ping(conn_a)

        # 5. Add bridge-b back
        write_config(conf_file, port1, port2, [("bridge-a", "topic-a2"), ("bridge-b", "topic-b")])
        broker.send_signal(signal.SIGHUP)
        conn_b = accept_bridge(sock, "bridge-b", "topic-b")
        check_forward(port2, conn_b, "bridge-b", "topic-b")
        mosq_test.do_ping(conn_a)

        rc = 0
    except mosq_test.TestError:
        pass
    except Exception:
        traceback.print_exc()
    finally:
        os.remove(conf_file)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if conn_a: conn_a.close()
        if conn_b: conn_b.close()
        sock.close()
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

do_test()
exit(0)
