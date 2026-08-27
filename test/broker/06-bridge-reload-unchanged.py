#!/usr/bin/env python3

# Regression test: a bridge with every bridge option set to a non-default
# value must not be restarted when the broker is reloaded with an unchanged
# config file. This catches bridge options that are not parsed on reload,
# which would make the running bridge compare unequal to the parsed default
# and be restarted with that default (e.g. bridge_require_ocsp, which would
# silently drop OCSP enforcement after SIGHUP).

from mosq_test_helper import *
import signal

def write_config(filename, port1, port2):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port2))
        f.write("allow_anonymous true\n")
        f.write("\n")
        f.write("connection bridge-full\n")
        f.write("address 127.0.0.1:%d 127.0.0.2:%d\n" % (port1, port1))
        f.write("round_robin true\n")
        f.write("topic in/# in 1 local/ remote/\n")
        f.write("topic out/# out 2\n")
        f.write("topic both/# both 0\n")
        f.write("remote_clientid remote-id\n")
        f.write("remote_username remote-user\n")
        f.write("remote_password remote-pass\n")
        f.write("local_clientid local-id\n")
        f.write("local_username local-user\n")
        f.write("local_password local-pass\n")
        f.write("cleansession false\n")
        f.write("local_cleansession true\n")
        f.write("keepalive_interval 30\n")
        f.write("notifications false\n")
        f.write("notifications_local_only true\n")
        f.write("notification_topic bridge/state\n")
        f.write("bridge_bind_address 127.0.0.1\n")
        f.write("start_type lazy\n")
        f.write("idle_timeout 120\n")
        f.write("restart_timeout 7 42\n")
        f.write("threshold 3\n")
        f.write("try_private false\n")
        f.write("bridge_attempt_unsubscribe false\n")
        f.write("bridge_outgoing_retain false\n")
        f.write("bridge_max_packet_size 1000\n")
        f.write("bridge_protocol_version mqttv50\n")
        f.write("bridge_cafile ../ssl/all-ca.crt\n")
        f.write("bridge_capath ../ssl\n")
        f.write("bridge_certfile ../ssl/client.crt\n")
        f.write("bridge_keyfile ../ssl/client.key\n")
        f.write("bridge_tls_version tlsv1.2\n")
        f.write("bridge_alpn mqtt\n")
        f.write("bridge_insecure true\n")
        f.write("bridge_require_ocsp true\n")


def do_test():
    (port1, port2) = mosq_test.get_port(2)
    conf_file = os.path.basename(__file__).replace('.py', '.conf')
    write_config(conf_file, port1, port2)

    rc = 1
    # Nothing listens on port1: the bridge is lazy and never connects, which
    # is fine, we only care whether the reload considers it changed.
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port2, use_conf=True)

    try:
        time.sleep(0.5)
        broker.send_signal(signal.SIGHUP)
        time.sleep(1)
        broker.send_signal(signal.SIGHUP)
        time.sleep(1)
        rc = 0
    finally:
        os.remove(conf_file)
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        log = stde.decode('utf-8')
        if rc == 0:
            if log.count("Reloading config.") != 2:
                print("FAIL: expected two reloads")
                rc = 1
            elif "configuration changed, restarting" in log:
                print("FAIL: unchanged bridge was restarted on reload")
                rc = 1
            elif "removed from config, stopping" in log:
                print("FAIL: unchanged bridge was removed on reload")
                rc = 1
            elif "Starting new bridge connection" in log:
                print("FAIL: unchanged bridge was started again on reload")
                rc = 1
        if rc:
            print(log)
            exit(rc)

do_test()
exit(0)
