#!/usr/bin/env python3

# Test $SYS/broker/subscribed_topics/<filter> subscriber counts, published when
# sys_subscribed_topics is true. +, # and % in filters are percent-encoded in
# the $SYS topic.

from mosq_test_helper import *
import signal

PREFIX = "$SYS/broker/subscribed_topics/"

CONF_FILE = os.path.basename(__file__).replace('.py', '.conf')

def write_config(filename, port, options):
    with open(filename, 'w') as f:
        f.write("listener %d\n" % (port))
        f.write("allow_anonymous true\n")
        for option in options:
            f.write("%s\n" % (option))

def encode(topic_filter):
    return topic_filter.replace("%", "%25").replace("+", "%2B").replace("#", "%23")

def count_publish(topic_filter, count, retain=False):
    return mosq_test.gen_publish(PREFIX + encode(topic_filter), qos=0, retain=retain, payload=str(count))

def expect_large_packet(sock, name, expected):
    # mosq_test.expect_packet() does a single recv(), which can return only
    # part of a large packet.
    recvd = b""
    while len(recvd) < len(expected):
        chunk = sock.recv(len(expected) - len(recvd))
        if not chunk:
            break
        recvd += chunk
    if not mosq_test.packet_matches(name, recvd, expected):
        raise mosq_test.TestError

def connect(client_id, port):
    connect_packet = mosq_test.gen_connect(client_id, keepalive=60)
    connack_packet = mosq_test.gen_connack(rc=0)
    return mosq_test.do_client_connect(connect_packet, connack_packet, port=port)

def subscribe(sock, mid, topic_filter):
    mosq_test.do_send_receive(sock, mosq_test.gen_subscribe(mid, topic_filter, 0),
            mosq_test.gen_suback(mid, 0), "suback %s" % (topic_filter[:40]))

def do_test(port):
    rc = 1
    monitor_filter = PREFIX + "#"
    long_filter = "l/" + "x"*(65535 - len(PREFIX))
    # Fits as is, but not once the # is encoded
    long_encoded_filter = "l/" + "y"*(65535 - len(PREFIX) - 5) + "/#"
    # Exactly the longest allowed topic name once encoded
    max_filter = "l/" + "z"*(65535 - len(PREFIX) - 6) + "/#"

    write_config(CONF_FILE, port, ["sys_subscribed_topics true"])
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    try:
        sock_m = connect("sys-monitor", port)
        # The monitor sees its own subscription, live and then as retained
        sock_m.send(mosq_test.gen_subscribe(1, monitor_filter, 0))
        mosq_test.expect_packet(sock_m, "monitor own count", count_publish(monitor_filter, 1))
        mosq_test.expect_packet(sock_m, "suback monitor", mosq_test.gen_suback(1, 0))
        mosq_test.expect_packet(sock_m, "monitor own count retained", count_publish(monitor_filter, 1, retain=True))

        sock_a = connect("sys-client-a", port)
        subscribe(sock_a, 2, "a/b")
        mosq_test.expect_packet(sock_m, "a/b 1", count_publish("a/b", 1))
        # Empty topic levels are reported in full
        subscribe(sock_a, 3, "a//b")
        mosq_test.expect_packet(sock_m, "a//b 1", count_publish("a//b", 1))
        subscribe(sock_a, 4, "x/y")
        mosq_test.expect_packet(sock_m, "x/y 1", count_publish("x/y", 1))
        # Too long for an MQTT topic name once prefixed, so not reported
        subscribe(sock_a, 5, long_filter)
        subscribe(sock_a, 10, long_encoded_filter)
        mosq_test.do_ping(sock_m, "no long filter count")
        subscribe(sock_a, 13, max_filter)
        expect_large_packet(sock_m, "max length 1", count_publish(max_filter, 1))
        # Wildcards and % are percent-encoded
        subscribe(sock_a, 11, "a/+/#")
        mosq_test.expect_packet(sock_m, "a/+/# 1", count_publish("a/+/#", 1))
        subscribe(sock_a, 12, "50%/x")
        mosq_test.expect_packet(sock_m, "50%/x 1", count_publish("50%/x", 1))

        sock_b = connect("sys-client-b", port)
        subscribe(sock_b, 6, "a/b")
        mosq_test.expect_packet(sock_m, "a/b 2", count_publish("a/b", 2))

        mosq_test.do_send_receive(sock_a, mosq_test.gen_unsubscribe(7, "a/b"), mosq_test.gen_unsuback(7), "unsuback a/b")
        mosq_test.expect_packet(sock_m, "a/b 1 after unsubscribe", count_publish("a/b", 1))

        # Last subscriber leaves: the count drops to 0
        sock_b.send(mosq_test.gen_disconnect())
        sock_b.close()
        mosq_test.expect_packet(sock_m, "a/b 0", count_publish("a/b", 0))

        # A late subscriber gets the counts as retained, including 0
        sock_c = connect("sys-client-c", port)
        sock_c.send(mosq_test.gen_subscribe(8, PREFIX + "a/b", 0))
        mosq_test.expect_packet(sock_c, "suback c a/b", mosq_test.gen_suback(8, 0))
        mosq_test.expect_packet(sock_c, "retained a/b 0", count_publish("a/b", 0, retain=True))
        mosq_test.expect_packet(sock_m, "c a/b monitor 1", count_publish(PREFIX + "a/b", 1))
        sock_c.send(mosq_test.gen_subscribe(9, PREFIX + "a//b", 0))
        mosq_test.expect_packet(sock_c, "suback c a//b", mosq_test.gen_suback(9, 0))
        mosq_test.expect_packet(sock_c, "retained a//b 1", count_publish("a//b", 1, retain=True))
        mosq_test.expect_packet(sock_m, "c a//b monitor 1", count_publish(PREFIX + "a//b", 1))

        # A client with several subscriptions leaves: all of its counts drop to 0
        sock_a.send(mosq_test.gen_disconnect())
        sock_a.close()
        mosq_test.expect_packet(sock_m, "a//b 0", count_publish("a//b", 0))
        mosq_test.expect_packet(sock_m, "x/y 0", count_publish("x/y", 0))
        expect_large_packet(sock_m, "max length 0", count_publish(max_filter, 0))
        mosq_test.expect_packet(sock_m, "a/+/# 0", count_publish("a/+/#", 0))
        mosq_test.expect_packet(sock_m, "50%/x 0", count_publish("50%/x", 0))
        mosq_test.do_ping(sock_m, "nothing more after a leaves")

        sock_c.close()
        sock_m.close()
        rc = 0
    except mosq_test.TestError:
        pass
    finally:
        os.remove(CONF_FILE)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

def do_test_disabled(port, options, reason):
    rc = 1
    write_config(CONF_FILE, port, options)
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    try:
        # Nothing is published, not even the monitor's own count
        sock_m = connect("sys-monitor", port)
        subscribe(sock_m, 1, PREFIX + "#")
        sock_a = connect("sys-client-a", port)
        subscribe(sock_a, 2, "a/b")
        mosq_test.do_ping(sock_m, "no counts: %s" % (reason))

        sock_a.close()
        sock_m.close()
        rc = 0
    except mosq_test.TestError:
        pass
    finally:
        os.remove(CONF_FILE)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

def do_test_reload(port):
    rc = 1
    write_config(CONF_FILE, port, ["sys_subscribed_topics true"])
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    try:
        sock_m = connect("sys-monitor", port)
        subscribe(sock_m, 1, "$SYS/broker/subscribed_topics/a/#")

        # sys_subscribed_topics isn't reloaded, and isn't affected by
        # sys_interval changing on reload
        write_config(CONF_FILE, port, ["sys_subscribed_topics false", "sys_interval 0"])
        broker.send_signal(signal.SIGHUP)
        mosq_test.do_ping(sock_m, "after reload")

        sock_a = connect("sys-client-a", port)
        subscribe(sock_a, 2, "a/b")
        mosq_test.expect_packet(sock_m, "a/b 1 after reload", count_publish("a/b", 1))

        sock_a.close()
        sock_m.close()
        rc = 0
    except mosq_test.TestError:
        pass
    finally:
        os.remove(CONF_FILE)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if rc == 0 and b"Reloading config." not in stde:
            print("FAIL: broker did not reload its config")
            rc = 1
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

port = mosq_test.get_port()
do_test(port)
do_test_disabled(port, [], "disabled by default")
do_test_disabled(port, ["sys_subscribed_topics true", "sys_interval 0"], "sys_interval 0")
do_test_reload(port)
exit(0)
