#!/usr/bin/env python3

# Test that a bridge added on reload whose local_clientid is in use by a
# connected ordinary client is refused without disturbing that client, and
# is started by a later reload once the client has disconnected. The
# disconnected client's session is then taken over by the bridge: its
# delayed Will must not be published while the bridge is running, and the
# listener's max_qos that applied to the client must not limit the bridge.

from mosq_test_helper import *
import signal
import traceback

def write_config(filename, port1, port2, with_bridge):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port2))
        f.write("max_qos 1\n")
        f.write("allow_anonymous true\n")
        if with_bridge:
            f.write("\n")
            f.write("connection bridge-c\n")
            f.write("remote_clientid bridge-c\n")
            f.write("local_clientid clash-id\n")
            f.write("address 127.0.0.1:%d\n" % (port1))
            f.write("topic c/# in 2\n")
            f.write("cleansession true\n")
            f.write("notifications false\n")
            f.write("restart_timeout 5\n")
            f.write("try_private false\n")
            f.write("bridge_protocol_version mqttv311\n")


def accept_bridge(sock):
    (conn, address) = sock.accept()
    conn.settimeout(20)
    connect_packet = mosq_test.gen_connect("bridge-c", keepalive=60, proto_ver=4)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=4)
    # The adopted session must not inherit the listener's max_qos 1: the
    # bridge subscribes with the configured QoS 2.
    subscribe_packet = mosq_test.gen_subscribe(1, "c/#", 2, proto_ver=4)
    suback_packet = mosq_test.gen_suback(1, 2, proto_ver=4)
    mosq_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)
    mosq_test.expect_packet(conn, "subscribe", subscribe_packet)
    conn.send(suback_packet)
    return conn


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
    write_config(conf_file, port1, port2, False)

    rc = 1

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(10)
    sock.bind(('', port1))
    sock.listen(5)

    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port2, use_conf=True)

    conn = None
    try:
        # Ordinary persistent MQTT v5 client with a delayed Will, holding the
        # id the bridge wants to use locally. On disconnect the Will is not
        # published but parked in the will-delay list for 2 seconds.
        will_props = mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_WILL_DELAY_INTERVAL, 2)
        connect_packet = mosq_test.gen_connect("clash-id", keepalive=60, proto_ver=5, clean_session=False,
                session_expiry=60, will_topic="clash/will", will_payload=b"stale-will", will_qos=0,
                will_properties=will_props)
        connack_props = mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, 10) \
            + mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_RECEIVE_MAXIMUM, 20) \
            + mqtt5_props.gen_byte_prop(mqtt5_props.PROP_MAXIMUM_QOS, 1)
        connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5, properties=connack_props, property_helper=False)
        client = mosq_test.do_client_connect(connect_packet, connack_packet, port=port2, timeout=10)

        write_config(conf_file, port1, port2, True)
        broker.send_signal(signal.SIGHUP)
        expect_no_connection(sock)
        # The client must be untouched
        mosq_test.do_ping(client)

        # Still refused on a second reload while the client is connected
        broker.send_signal(signal.SIGHUP)
        expect_no_connection(sock)
        mosq_test.do_ping(client)

        # Watch for the old client's Will; it must never be published once
        # the bridge has taken the session over.
        connect_packet = mosq_test.gen_connect("will-watcher", keepalive=60)
        connack_packet = mosq_test.gen_connack(rc=0)
        subscribe_packet = mosq_test.gen_subscribe(3, "clash/will", 0)
        suback_packet = mosq_test.gen_suback(3, 0)
        watcher = mosq_test.do_client_connect(connect_packet, connack_packet, port=port2, timeout=10)
        mosq_test.do_send_receive(watcher, subscribe_packet, suback_packet, "suback")

        # Once the client is gone, the next reload starts the bridge, taking
        # over the persistent session.
        client.close()
        time.sleep(0.5)
        broker.send_signal(signal.SIGHUP)
        conn = accept_bridge(sock)

        # Outlive the will delay, then drop the remote side too: neither the
        # delay timer nor the bridge disconnect may publish the client's Will.
        watcher.settimeout(4)
        try:
            data = watcher.recv(100)
            raise mosq_test.TestError("Stale client Will published while bridge running: %r" % (data,))
        except socket.timeout:
            pass
        conn.close()
        conn = None
        watcher.settimeout(2)
        try:
            data = watcher.recv(100)
            raise mosq_test.TestError("Stale client Will published after bridge disconnect: %r" % (data,))
        except socket.timeout:
            pass
        watcher.settimeout(10)
        mosq_test.do_ping(watcher)
        watcher.close()
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
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

do_test()
exit(0)
