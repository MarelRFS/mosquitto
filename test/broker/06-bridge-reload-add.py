#!/usr/bin/env python3

# Test that new bridge connections added to the config file are started when
# the broker receives SIGHUP, that a second SIGHUP with an unchanged config
# does not start a duplicate bridge, and that an existing bridge is left
# running when another bridge is added.

from mosq_test_helper import *
import signal

def write_config(filename, port1, port2, bridges):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port2))
        f.write("allow_anonymous true\n")
        for name in bridges:
            f.write("\n")
            f.write("connection %s\n" % (name))
            f.write("remote_clientid %s\n" % (name))
            f.write("address 127.0.0.1:%d\n" % (port1))
            f.write("topic %s/# out\n" % (name))
            f.write("cleansession true\n")
            f.write("notifications false\n")
            f.write("restart_timeout 5\n")
            f.write("try_private false\n")
            f.write("bridge_protocol_version mqttv311\n")


def accept_bridge(sock, name):
    (conn, address) = sock.accept()
    conn.settimeout(20)

    connect_packet = mosq_test.gen_connect(name, keepalive=60, proto_ver=4)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=4)
    unsubscribe_packet = mosq_test.gen_unsubscribe(1, "%s/#" % (name), proto_ver=4)
    unsuback_packet = mosq_test.gen_unsuback(1, proto_ver=4)

    mosq_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)
    mosq_test.expect_packet(conn, "unsubscribe", unsubscribe_packet)
    conn.send(unsuback_packet)
    return conn


def check_forward(local_port, conn, name, mid):
    # Publish locally, expect it to arrive on the fake remote broker.
    payload = "reload-%s" % (name)
    publish_local = mosq_test.gen_publish("%s/msg" % (name), qos=0, payload=payload)
    publish_remote = mosq_test.gen_publish("%s/msg" % (name), qos=0, payload=payload)

    connect_packet = mosq_test.gen_connect("pub-%s" % (name), keepalive=60)
    connack_packet = mosq_test.gen_connack(rc=0)
    lsock = mosq_test.do_client_connect(connect_packet, connack_packet, port=local_port, timeout=10)
    lsock.send(publish_local)
    lsock.close()

    mosq_test.expect_packet(conn, "forwarded publish", publish_remote)


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


def do_test():
    (port1, port2) = mosq_test.get_port(2)
    conf_file = os.path.basename(__file__).replace('.py', '.conf')
    write_config(conf_file, port1, port2, [])

    rc = 1

    if os.environ.get('MOSQ_USE_VALGRIND') is not None:
        sleep_time = 5
    else:
        sleep_time = 0.5

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(10)
    sock.bind(('', port1))
    sock.listen(5)

    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port2, use_conf=True)
    time.sleep(sleep_time)

    conn1 = None
    conn2 = None
    try:
        # No bridge configured at start
        expect_no_connection(sock)

        # Add a bridge and reload
        write_config(conf_file, port1, port2, ["bridge-a"])
        broker.send_signal(signal.SIGHUP)
        conn1 = accept_bridge(sock, "bridge-a")
        check_forward(port2, conn1, "bridge-a", 1)

        # Reload again with unchanged config: no duplicate bridge
        broker.send_signal(signal.SIGHUP)
        expect_no_connection(sock)
        mosq_test.do_ping(conn1)

        # Add a second bridge, first must stay connected
        write_config(conf_file, port1, port2, ["bridge-a", "bridge-b"])
        broker.send_signal(signal.SIGHUP)
        conn2 = accept_bridge(sock, "bridge-b")
        check_forward(port2, conn2, "bridge-b", 2)
        check_forward(port2, conn1, "bridge-a", 3)

        rc = 0
    except mosq_test.TestError:
        pass
    finally:
        os.remove(conf_file)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if conn1: conn1.close()
        if conn2: conn2.close()
        sock.close()
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

do_test()
exit(0)
