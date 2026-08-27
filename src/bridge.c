/*
Copyright (c) 2009-2020 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include "config.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#ifndef WIN32
#include <netdb.h>
#include <sys/socket.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#ifndef WIN32
#include <unistd.h>
#else
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include "mqtt_protocol.h"
#include "mosquitto.h"
#include "mosquitto_broker_internal.h"
#include "mosquitto_internal.h"
#include "net_mosq.h"
#include "memory_mosq.h"
#include "packet_mosq.h"
#include "send_mosq.h"
#include "time_mosq.h"
#include "tls_mosq.h"
#include "util_mosq.h"
#include "alias_mosq.h"
#include "will_mosq.h"

#ifdef WITH_BRIDGE

static void bridge__backoff_step(struct mosquitto *context);
static void bridge__backoff_reset(struct mosquitto *context);

static void bridge__start_new(int first);

void bridge__start_all(void)
{
	bridge__start_new(0);
}


static bool bridge__str_eq(const char *a, const char *b)
{
	if(a == NULL && b == NULL) return true;
	if(a == NULL || b == NULL) return false;
	return strcmp(a, b) == 0;
}


/* Compare the configuration of a running bridge with a freshly parsed one.
 * Only configuration is compared; fields that the bridge mutates at runtime
 * (cur_address, restart_t, lazy_reconnect, protocol downgrade, backoff jitter
 * in restart_timeout, resolved clean_start_local, ...) are ignored or
 * compared via their configured equivalents. */
static bool bridge__config_equal(const struct mosquitto__bridge *old_bridge, const struct mosquitto__bridge *new_bridge)
{
	int i;
	int8_t old_csl, new_csl;

	if(old_bridge->address_count != new_bridge->address_count) return false;
	for(i=0; i<old_bridge->address_count; i++){
		if(!bridge__str_eq(old_bridge->addresses[i].address, new_bridge->addresses[i].address)) return false;
		if(old_bridge->addresses[i].port != new_bridge->addresses[i].port) return false;
	}

	if(old_bridge->topic_count != new_bridge->topic_count) return false;
	for(i=0; i<old_bridge->topic_count; i++){
		if(!bridge__str_eq(old_bridge->topics[i].topic, new_bridge->topics[i].topic)) return false;
		if(!bridge__str_eq(old_bridge->topics[i].local_prefix, new_bridge->topics[i].local_prefix)) return false;
		if(!bridge__str_eq(old_bridge->topics[i].remote_prefix, new_bridge->topics[i].remote_prefix)) return false;
		if(old_bridge->topics[i].direction != new_bridge->topics[i].direction) return false;
		if(old_bridge->topics[i].qos != new_bridge->topics[i].qos) return false;
	}

	if(!bridge__str_eq(old_bridge->remote_clientid, new_bridge->remote_clientid)) return false;
	if(!bridge__str_eq(old_bridge->remote_username, new_bridge->remote_username)) return false;
	if(!bridge__str_eq(old_bridge->remote_password, new_bridge->remote_password)) return false;
	if(!bridge__str_eq(old_bridge->local_clientid, new_bridge->local_clientid)) return false;
	if(!bridge__str_eq(old_bridge->local_username, new_bridge->local_username)) return false;
	if(!bridge__str_eq(old_bridge->local_password, new_bridge->local_password)) return false;
	if(!bridge__str_eq(old_bridge->notification_topic, new_bridge->notification_topic)) return false;
	if(!bridge__str_eq(old_bridge->bind_address, new_bridge->bind_address)) return false;

	if(old_bridge->round_robin != new_bridge->round_robin) return false;
	if(old_bridge->try_private != new_bridge->try_private) return false;
	if(old_bridge->clean_start != new_bridge->clean_start) return false;
	old_csl = old_bridge->clean_start_local == -1 ? (int8_t)old_bridge->clean_start : old_bridge->clean_start_local;
	new_csl = new_bridge->clean_start_local == -1 ? (int8_t)new_bridge->clean_start : new_bridge->clean_start_local;
	if(old_csl != new_csl) return false;
	if(old_bridge->keepalive != new_bridge->keepalive) return false;
	if(old_bridge->protocol_version_cfg != new_bridge->protocol_version_cfg) return false;
	if(old_bridge->notifications != new_bridge->notifications) return false;
	if(old_bridge->notifications_local_only != new_bridge->notifications_local_only) return false;
	if(old_bridge->start_type != new_bridge->start_type) return false;
	if(old_bridge->idle_timeout != new_bridge->idle_timeout) return false;
	if(old_bridge->backoff_base != new_bridge->backoff_base) return false;
	if(old_bridge->backoff_cap != new_bridge->backoff_cap) return false;
	/* With backoff enabled restart_timeout holds the current jittered value. */
	if(new_bridge->backoff_cap == 0 && old_bridge->restart_timeout != new_bridge->restart_timeout) return false;
	if(old_bridge->threshold != new_bridge->threshold) return false;
	if(old_bridge->maximum_packet_size != new_bridge->maximum_packet_size) return false;
	if(old_bridge->attempt_unsubscribe != new_bridge->attempt_unsubscribe) return false;
	if(old_bridge->outgoing_retain != new_bridge->outgoing_retain) return false;

#ifdef WITH_TLS
	if(old_bridge->tls_insecure != new_bridge->tls_insecure) return false;
	if(old_bridge->tls_ocsp_required != new_bridge->tls_ocsp_required) return false;
	if(!bridge__str_eq(old_bridge->tls_cafile, new_bridge->tls_cafile)) return false;
	if(!bridge__str_eq(old_bridge->tls_capath, new_bridge->tls_capath)) return false;
	if(!bridge__str_eq(old_bridge->tls_certfile, new_bridge->tls_certfile)) return false;
	if(!bridge__str_eq(old_bridge->tls_keyfile, new_bridge->tls_keyfile)) return false;
	if(!bridge__str_eq(old_bridge->tls_version, new_bridge->tls_version)) return false;
	if(!bridge__str_eq(old_bridge->tls_alpn, new_bridge->tls_alpn)) return false;
#ifdef FINAL_WITH_TLS_PSK
	if(!bridge__str_eq(old_bridge->tls_psk_identity, new_bridge->tls_psk_identity)) return false;
	if(!bridge__str_eq(old_bridge->tls_psk, new_bridge->tls_psk)) return false;
#endif
#endif
	return true;
}


/* Stop and remove live bridges that are no longer in new_cfg, or whose
 * configuration differs from new_cfg (those are then re-added as new bridges
 * by bridge__merge_new()). */
static void bridge__remove_stale(struct mosquitto__config *new_cfg, struct mosquitto__config *live)
{
	int i, j;
	struct mosquitto__bridge *bridge, *new_bridge;
	struct mosquitto *context;

	for(i=live->bridge_count-1; i>=0; i--){
		bridge = live->bridges[i];
		new_bridge = NULL;
		for(j=0; j<new_cfg->bridge_count; j++){
			if(!strcmp(bridge->name, new_cfg->bridges[j]->name)){
				new_bridge = new_cfg->bridges[j];
				break;
			}
		}
		if(new_bridge && bridge__config_equal(bridge, new_bridge)){
			continue;
		}
		if(new_bridge){
			log__printf(NULL, MOSQ_LOG_INFO, "Bridge %s configuration changed, restarting.", bridge->name);
		}else{
			log__printf(NULL, MOSQ_LOG_INFO, "Bridge %s removed from config, stopping.", bridge->name);
		}

		for(j=0; j<db.bridge_count; j++){
			context = db.bridges[j];
			if(context && context->bridge == bridge){
				bridge__destroy(context);
				break;
			}
		}
		config__bridge_free(bridge);
		config__bridge_remove(live, i);
	}
}


/* Move bridges from src that are not already configured in dest (matched by
 * connection name) into dest, and free the rest. bridge__remove_stale() has
 * already removed changed or deleted bridges from dest, and
 * config__check_bridges() has already rejected local_clientid clashes within
 * src. The caller must have reserved room in dest for all of src's bridges
 * with config__reserve_bridges(), so this cannot fail part way through. Takes
 * ownership of src's bridges. */
static void bridge__merge_new(struct mosquitto__config *src, struct mosquitto__config *dest)
{
	int i, j;
	bool present;

	for(i=0; i<src->bridge_count; i++){
		present = false;
		for(j=0; j<dest->bridge_count; j++){
			if(!strcmp(src->bridges[i]->name, dest->bridges[j]->name)){
				present = true;
				break;
			}
		}
		if(!present && dest->bridge_count >= dest->bridge_capacity){
			/* Cannot happen if the caller reserved capacity; don't leave a
			 * bridge in the config that was never started. */
			log__printf(NULL, MOSQ_LOG_ERR, "Error: No room for bridge \"%s\", ignoring.", src->bridges[i]->name);
			present = true;
		}
		if(present){
			config__bridge_free(src->bridges[i]);
		}else{
			dest->bridges[dest->bridge_count] = src->bridges[i];
			dest->bridge_count++;
		}
		src->bridges[i] = NULL;
	}
	mosquitto__free(src->bridges);
	src->bridges = NULL;
	src->bridge_count = 0;
	src->bridge_capacity = 0;
}


/* Start bridges from index first onwards in db.config->bridges; those before
 * it are already running and must not be touched. Used at startup (first=0)
 * and for bridges appended by a reload. */
static void bridge__start_new(int first)
{
	int i;
	int rc;
	struct mosquitto *context;

	for(i=first; i<db.config->bridge_count; i++){
		context = NULL;
		rc = bridge__new(db.config->bridges[i], &context);
		if(context == NULL){
			/* bridge__new() failed before creating or publishing a context, so
			 * nothing references this bridge config. Drop it from the live
			 * config so the next reload sees it as new and tries again,
			 * rather than skipping it by name forever (this also applies at
			 * startup). Once a context exists
			 * and is tracked in db.bridges (even if the connect attempt
			 * failed with e.g. MOSQ_ERR_NOMEM), it must be kept: bridge_check()
			 * retries it and it owns pointers into this config. */
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Unable to create bridge %s (%s), it will be retried on the next reload.",
					db.config->bridges[i]->name, mosquitto_strerror(rc));
			config__bridge_free(db.config->bridges[i]);
			config__bridge_remove(db.config, i);
			i--;
			continue;
		}
		if(rc > 0){
			log__printf(NULL, MOSQ_LOG_WARNING, "Warning: Unable to connect to bridge %s.",
					db.config->bridges[i]->name);
		}
		/* bridge__register_local_connections() only runs before the main loop,
		 * so register the socket with the mux here if a connection is up or
		 * pending. If not connected, bridge_check() will retry and register. */
		if(context->sock != INVALID_SOCKET){
			if(mux__add_in(context)){
				log__printf(NULL, MOSQ_LOG_ERR, "Error registering new bridge %s: %s",
						db.config->bridges[i]->name, strerror(errno));
			}else{
				mux__add_out(context);
			}
		}
	}
}


/* Apply a reloaded bridge configuration to the running set. Bridges are
 * matched by connection name: removed and changed bridges are stopped and
 * freed, new (and changed) ones are appended to live and started, unchanged
 * ones are left running. new_cfg must already have passed
 * config__validate_bridges() and config__check_bridges(). On success the
 * bridges of new_cfg have been consumed; on failure nothing has been changed
 * and the caller still owns them. */
int bridge__reload(struct mosquitto__config *new_cfg, struct mosquitto__config *live)
{
	int rc;
	int first_new;

	/* Reserve room for the worst case (every parsed bridge is new) before any
	 * running bridge is torn down, so the merge cannot fail and leave the
	 * live set partially updated. */
	rc = config__reserve_bridges(live, live->bridge_count + new_cfg->bridge_count);
	if(rc) return rc;

	bridge__remove_stale(new_cfg, live);
	first_new = live->bridge_count;
	bridge__merge_new(new_cfg, live);
	/* Start the newly committed bridges right away, before the caller does
	 * any further fallible processing, so that nothing is ever left
	 * committed but unstarted. */
	if(first_new < live->bridge_count){
		bridge__start_new(first_new);
	}
	return MOSQ_ERR_SUCCESS;
}


/* Stop a running bridge and free its context. The bridge config struct
 * (context->bridge) is left for the caller to remove from db.config->bridges
 * and free with config__bridge_free(). Used when a bridge is removed from, or
 * changed in, the config file and the broker is reloaded. */
void bridge__destroy(struct mosquitto *context)
{
	struct mosquitto__bridge *bridge;
	char *notification_topic;
	size_t notification_topic_len;

	if(!context || !context->bridge) return;
	bridge = context->bridge;

	log__printf(NULL, MOSQ_LOG_INFO, "Stopping bridge connection %s.", bridge->name);

	if(bridge->primary_retry_sock != INVALID_SOCKET){
		COMPAT_CLOSE(bridge->primary_retry_sock);
		bridge->primary_retry_sock = INVALID_SOCKET;
	}

	/* Don't publish a retained '0' will for a bridge that no longer exists;
	 * instead clear the retained state message locally. The remote broker
	 * will still publish our will when the socket closes. */
	will__clear(context);
	if(bridge->notifications && bridge->remote_clientid){
		if(bridge->notification_topic){
			db__messages_easy_queue(NULL, bridge->notification_topic, 0, 0, NULL, 1, 0, NULL);
		}else{
			notification_topic_len = strlen(bridge->remote_clientid)+strlen("$SYS/broker/connection//state");
			notification_topic = mosquitto__malloc(sizeof(char)*(notification_topic_len+1));
			if(notification_topic){
				snprintf(notification_topic, notification_topic_len+1, "$SYS/broker/connection/%s/state", bridge->remote_clientid);
				db__messages_easy_queue(NULL, notification_topic, 0, 0, NULL, 1, 0, NULL);
				mosquitto__free(notification_topic);
			}
		}
	}

	if(context->sock != INVALID_SOCKET){
		mux__delete(context);
	}
	session_expiry__remove(context);
	will_delay__remove(context);

#if defined(__GLIBC__) && defined(WITH_ADNS)
	if(context->adns){
		/* An asynchronous DNS lookup may still be running in a resolver
		 * thread that writes into context->adns. Unlike at shutdown, the
		 * broker keeps running after a reload, so we must not free the
		 * request until it has completed or been cancelled. */
		if(gai_cancel(context->adns) == EAI_NOTCANCELED){
			const struct gaicb *list[1];

			list[0] = context->adns;
			log__printf(NULL, MOSQ_LOG_INFO, "Waiting for DNS lookup of bridge %s to finish before stopping it.", bridge->name);
			while(gai_suspend(list, 1, NULL) == EAI_INTR){
			}
		}
		if(gai_error(context->adns) == 0 && context->adns->ar_result){
			freeaddrinfo(context->adns->ar_result);
		}
		mosquitto__free((struct addrinfo *)context->adns->ar_request);
		mosquitto__free(context->adns);
		context->adns = NULL;
	}
#endif

	/* Calls bridge__cleanup() (clears the db.bridges slot), closes the
	 * socket, discards the session and queued messages, and frees context. */
	context__cleanup(context, true);
}


int bridge__new(struct mosquitto__bridge *bridge, struct mosquitto **context_out)
{
	struct mosquitto *new_context = NULL;
	struct mosquitto **bridges;
	char *local_id;
	int slot;
	bool in_use;

	assert(bridge);
	if(context_out) *context_out = NULL;

	local_id = mosquitto__strdup(bridge->local_clientid);
	if(!local_id){
		return MOSQ_ERR_NOMEM;
	}

	HASH_FIND(hh_id, db.contexts_by_id, local_id, strlen(local_id), new_context);
	if(new_context){
		/* A context with this id already exists. At startup this is a session
		 * restored from the persistence file, which we take over. At reload
		 * time it may instead be a connected ordinary client, or another
		 * running bridge, using this id. Taking over a live context would
		 * leave its socket registered in the mux and socket hash while
		 * bridge__connect() resets it, so refuse without touching it. The
		 * bridge is dropped from the config by the caller and retried on the
		 * next reload. */
		in_use = (new_context->sock != INVALID_SOCKET || new_context->bridge != NULL);
#ifdef WITH_WEBSOCKETS
		if(new_context->wsi) in_use = true;
#endif
		if(in_use){
			log__printf(NULL, MOSQ_LOG_ERR, "Error: Unable to start bridge %s, its local_clientid '%s' is in use by a connected client.",
					bridge->name, local_id);
			mosquitto__free(local_id);
			return MOSQ_ERR_INVAL;
		}
	}

	/* Reserve the db.bridges slot before creating or publishing the context,
	 * so that a failure below leaves no context that references this bridge
	 * without being tracked in db.bridges. Reuse a slot freed by
	 * bridge__destroy() if there is one. */
	for(slot=0; slot<db.bridge_count; slot++){
		if(db.bridges[slot] == NULL){
			break;
		}
	}
	if(slot == db.bridge_count){
		bridges = mosquitto__realloc(db.bridges, (size_t)(db.bridge_count+1)*sizeof(struct mosquitto *));
		if(!bridges){
			mosquitto__free(local_id);
			return MOSQ_ERR_NOMEM;
		}
		db.bridges = bridges;
		db.bridges[slot] = NULL;
		db.bridge_count++;
	}

	if(new_context){
		/* Take over the disconnected session (possible from persistent db, or
		 * at reload a client that has since disconnected). Drop the state the
		 * old client left behind before this context becomes a bridge: it
		 * must not expire while the bridge runs, must not publish the
		 * client's will, and its credentials are replaced by the bridge's. */
		session_expiry__remove(new_context);
		will_delay__remove(new_context);
		will__clear(new_context);
		alias__free_all(new_context);
		/* max_qos came from the client's listener; a bridge is limited only
		 * by what the remote grants in its CONNACK. */
		new_context->max_qos = 2;
		mosquitto__free(new_context->username);
		new_context->username = NULL;
		mosquitto__free(new_context->password);
		new_context->password = NULL;
		mosquitto__free(local_id);
	}else{
		/* id wasn't found, so generate a new context */
		new_context = context__init(INVALID_SOCKET);
		if(!new_context){
			/* The reserved slot stays NULL, which every user of db.bridges skips. */
			mosquitto__free(local_id);
			return MOSQ_ERR_NOMEM;
		}
		new_context->id = local_id;
		context__add_to_by_id(new_context);
	}
	db.bridges[slot] = new_context;
	new_context->bridge = bridge;
	new_context->is_bridge = true;

	new_context->username = new_context->bridge->remote_username;
	new_context->password = new_context->bridge->remote_password;

#ifdef WITH_TLS
	new_context->tls_cafile = new_context->bridge->tls_cafile;
	new_context->tls_capath = new_context->bridge->tls_capath;
	new_context->tls_certfile = new_context->bridge->tls_certfile;
	new_context->tls_keyfile = new_context->bridge->tls_keyfile;
	new_context->tls_cert_reqs = SSL_VERIFY_PEER;
	new_context->tls_ocsp_required = new_context->bridge->tls_ocsp_required;
	new_context->tls_version = new_context->bridge->tls_version;
	new_context->tls_insecure = new_context->bridge->tls_insecure;
	new_context->tls_alpn = new_context->bridge->tls_alpn;
	new_context->tls_engine = db.config->default_listener.tls_engine;
	new_context->tls_keyform = db.config->default_listener.tls_keyform;
	new_context->ssl_ctx_defaults = true;
#ifdef FINAL_WITH_TLS_PSK
	new_context->tls_psk_identity = new_context->bridge->tls_psk_identity;
	new_context->tls_psk = new_context->bridge->tls_psk;
#endif
#endif

	bridge->try_private_accepted = true;
	if(bridge->clean_start_local == -1){
		/* default to "regular" clean start setting */
		bridge->clean_start_local = bridge->clean_start;
	}
	new_context->retain_available = bridge->outgoing_retain;
	new_context->protocol = bridge->protocol_version;
	if(!bridge->clean_start_local){
		new_context->session_expiry_interval = UINT32_MAX;
		if(new_context->expiry_list_item){
			/* We've restored from persistence and been added to the session
			 * expiry list, even though we should never be expired */
			session_expiry__remove(new_context);
		}
	}

	if(context_out) *context_out = new_context;

#if defined(__GLIBC__) && defined(WITH_ADNS)
	new_context->bridge->restart_t = 1; /* force quick restart of bridge */
	return bridge__connect_step1(new_context);
#else
	return bridge__connect(new_context);
#endif
}

#if defined(__GLIBC__) && defined(WITH_ADNS)
int bridge__connect_step1(struct mosquitto *context)
{
	int rc;
	char *notification_topic;
	size_t notification_topic_len;
	uint8_t notification_payload;
	int i;
	uint8_t qos;

	if(!context || !context->bridge) return MOSQ_ERR_INVAL;

	mosquitto__set_state(context, mosq_cs_new);
	context->sock = INVALID_SOCKET;
	context->last_msg_in = db.now_s;
	context->next_msg_out = db.now_s + context->bridge->keepalive;
	context->keepalive = context->bridge->keepalive;
	context->clean_start = context->bridge->clean_start;
	context->in_packet.payload = NULL;
	context->ping_t = 0;
	context->bridge->lazy_reconnect = false;
	context->maximum_packet_size = context->bridge->maximum_packet_size;
	bridge__packet_cleanup(context);
	db__message_reconnect_reset(context);

	db__messages_delete(context, false);

	/* Delete all local subscriptions even for clean_start==false. We don't
	 * remove any messages and the next loop carries out the resubscription
	 * anyway. This means any unwanted subs will be removed.
	 */
	sub__clean_session(context);

	for(i=0; i<context->bridge->topic_count; i++){
		if(context->bridge->topics[i].direction == bd_out || context->bridge->topics[i].direction == bd_both){
			log__printf(NULL, MOSQ_LOG_DEBUG, "Bridge %s doing local SUBSCRIBE on topic %s", context->id, context->bridge->topics[i].local_topic);
			if(context->bridge->topics[i].qos > context->max_qos){
				qos = context->max_qos;
			}else{
				qos = context->bridge->topics[i].qos;
			}
			if(sub__add(context,
						context->bridge->topics[i].local_topic,
						qos,
						0,
						MQTT_SUB_OPT_NO_LOCAL | MQTT_SUB_OPT_RETAIN_AS_PUBLISHED) > 0){
				return 1;
			}
			retain__queue(context,
					context->bridge->topics[i].local_topic,
					qos, 0);
		}
	}

	/* prepare backoff for a possible failure. Restart timeout will be reset if connection gets established */
	bridge__backoff_step(context);

	if(context->bridge->notifications){
		if(context->max_qos == 0){
			qos = 0;
		}else{
			qos = 1;
		}
		if(context->bridge->notification_topic){
			if(!context->bridge->initial_notification_done){
				notification_payload = '0';
				db__messages_easy_queue(context, context->bridge->notification_topic, qos, 1, &notification_payload, 1, 0, NULL);
				context->bridge->initial_notification_done = true;
			}
			notification_payload = '0';
			rc = will__set(context, context->bridge->notification_topic, 1, &notification_payload, qos, true, NULL);
			if(rc != MOSQ_ERR_SUCCESS){
				return rc;
			}
		}else{
			notification_topic_len = strlen(context->bridge->remote_clientid)+strlen("$SYS/broker/connection//state");
			notification_topic = mosquitto__malloc(sizeof(char)*(notification_topic_len+1));
			if(!notification_topic) return MOSQ_ERR_NOMEM;

			snprintf(notification_topic, notification_topic_len+1, "$SYS/broker/connection/%s/state", context->bridge->remote_clientid);

			if(!context->bridge->initial_notification_done){
				notification_payload = '0';
				db__messages_easy_queue(context, notification_topic, qos, 1, &notification_payload, 1, 0, NULL);
				context->bridge->initial_notification_done = true;
			}

			notification_payload = '0';
			rc = will__set(context, notification_topic, 1, &notification_payload, qos, true, NULL);
			mosquitto__free(notification_topic);
			if(rc != MOSQ_ERR_SUCCESS){
				return rc;
			}
		}
	}

	log__printf(NULL, MOSQ_LOG_NOTICE, "Connecting bridge (step 1) %s (%s:%d)", context->bridge->name, context->bridge->addresses[context->bridge->cur_address].address, context->bridge->addresses[context->bridge->cur_address].port);
	rc = net__try_connect_step1(context, context->bridge->addresses[context->bridge->cur_address].address);
	if(rc > 0 ){
		if(rc == MOSQ_ERR_TLS){
			mux__delete(context);
			net__socket_close(context);
			return rc; /* Error already printed */
		}else if(rc == MOSQ_ERR_ERRNO){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", strerror(errno));
		}else if(rc == MOSQ_ERR_EAI){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", gai_strerror(errno));
		}

		return rc;
	}

	return MOSQ_ERR_SUCCESS;
}


int bridge__connect_step2(struct mosquitto *context)
{
	int rc;

	if(!context || !context->bridge) return MOSQ_ERR_INVAL;

	log__printf(NULL, MOSQ_LOG_NOTICE, "Connecting bridge (step 2) %s (%s:%d)", context->bridge->name, context->bridge->addresses[context->bridge->cur_address].address, context->bridge->addresses[context->bridge->cur_address].port);
	rc = net__try_connect_step2(context, context->bridge->addresses[context->bridge->cur_address].port, &context->sock);
	if(rc > 0){
		if(rc == MOSQ_ERR_TLS){
			mux__delete(context);
			net__socket_close(context);
			return rc; /* Error already printed */
		}else if(rc == MOSQ_ERR_ERRNO){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", strerror(errno));
		}else if(rc == MOSQ_ERR_EAI){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", gai_strerror(errno));
		}

		return rc;
	}

	HASH_ADD(hh_sock, db.contexts_by_sock, sock, sizeof(context->sock), context);

	if(rc == MOSQ_ERR_CONN_PENDING){
		mosquitto__set_state(context, mosq_cs_connect_pending);
		mux__add_out(context);
	}
	return rc;
}


int bridge__connect_step3(struct mosquitto *context)
{
	int rc;

	rc = net__socket_connect_step3(context, context->bridge->addresses[context->bridge->cur_address].address);
	if(rc > 0){
		if(rc == MOSQ_ERR_TLS){
			mux__delete(context);
			net__socket_close(context);
			return rc; /* Error already printed */
		}else if(rc == MOSQ_ERR_ERRNO){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", strerror(errno));
		}else if(rc == MOSQ_ERR_EAI){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", gai_strerror(errno));
		}

		return rc;
	}

	if(context->bridge->round_robin == false && context->bridge->cur_address != 0){
		context->bridge->primary_retry = db.now_s + 5;
	}

	rc = send__connect(context, context->keepalive, context->clean_start, NULL);
	if(rc == MOSQ_ERR_SUCCESS){
		return MOSQ_ERR_SUCCESS;
	}else if(rc == MOSQ_ERR_ERRNO && errno == ENOTCONN){
		return MOSQ_ERR_SUCCESS;
	}else{
		if(rc == MOSQ_ERR_TLS){
			return rc; /* Error already printed */
		}else if(rc == MOSQ_ERR_ERRNO){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", strerror(errno));
		}else if(rc == MOSQ_ERR_EAI){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", gai_strerror(errno));
		}
		mux__delete(context);
		net__socket_close(context);
		return rc;
	}
}
#else

int bridge__connect(struct mosquitto *context)
{
	int rc, rc2;
	int i;
	char *notification_topic = NULL;
	size_t notification_topic_len;
	uint8_t notification_payload;
	uint8_t qos;

	if(!context || !context->bridge) return MOSQ_ERR_INVAL;

	mosquitto__set_state(context, mosq_cs_new);
	context->sock = INVALID_SOCKET;
	context->last_msg_in = db.now_s;
	context->next_msg_out = db.now_s + context->bridge->keepalive;
	context->keepalive = context->bridge->keepalive;
	context->clean_start = context->bridge->clean_start;
	context->in_packet.payload = NULL;
	context->ping_t = 0;
	context->bridge->lazy_reconnect = false;
	context->maximum_packet_size = context->bridge->maximum_packet_size;
	bridge__packet_cleanup(context);
	db__message_reconnect_reset(context);

	db__messages_delete(context, false);

	/* Delete all local subscriptions even for clean_start==false. We don't
	 * remove any messages and the next loop carries out the resubscription
	 * anyway. This means any unwanted subs will be removed.
	 */
	sub__clean_session(context);

	for(i=0; i<context->bridge->topic_count; i++){
		if(context->bridge->topics[i].direction == bd_out || context->bridge->topics[i].direction == bd_both){
			log__printf(NULL, MOSQ_LOG_DEBUG, "Bridge %s doing local SUBSCRIBE on topic %s", context->id, context->bridge->topics[i].local_topic);
			if(context->bridge->topics[i].qos > context->max_qos){
				qos = context->max_qos;
			}else{
				qos = context->bridge->topics[i].qos;
			}
			if(sub__add(context,
						context->bridge->topics[i].local_topic,
						qos,
						0,
						MQTT_SUB_OPT_NO_LOCAL | MQTT_SUB_OPT_RETAIN_AS_PUBLISHED) > 0){

				return 1;
			}
		}
	}

	/* prepare backoff for a possible failure. Restart timeout will be reset if connection gets established */
	bridge__backoff_step(context);

	if(context->bridge->notifications){
		if(context->max_qos == 0){
			qos = 0;
		}else{
			qos = 1;
		}
		if(context->bridge->notification_topic){
			if(!context->bridge->initial_notification_done){
				notification_payload = '0';
				db__messages_easy_queue(context, context->bridge->notification_topic, qos, 1, &notification_payload, 1, 0, NULL);
				context->bridge->initial_notification_done = true;
			}

			notification_payload = '0';
			rc = will__set(context, context->bridge->notification_topic, 1, &notification_payload, qos, true, NULL);
			if(rc != MOSQ_ERR_SUCCESS){
				return rc;
			}
		}else{
			notification_topic_len = strlen(context->bridge->remote_clientid)+strlen("$SYS/broker/connection//state");
			notification_topic = mosquitto__malloc(sizeof(char)*(notification_topic_len+1));
			if(!notification_topic) return MOSQ_ERR_NOMEM;

			snprintf(notification_topic, notification_topic_len+1, "$SYS/broker/connection/%s/state", context->bridge->remote_clientid);

			if(!context->bridge->initial_notification_done){
				notification_payload = '0';
				db__messages_easy_queue(context, notification_topic, qos, 1, &notification_payload, 1, 0, NULL);
				context->bridge->initial_notification_done = true;
			}

			notification_payload = '0';
			rc = will__set(context, notification_topic, 1, &notification_payload, qos, true, NULL);
			if(rc != MOSQ_ERR_SUCCESS){
				mosquitto__free(notification_topic);
				return rc;
			}
			mosquitto__free(notification_topic);
		}
	}

	log__printf(NULL, MOSQ_LOG_NOTICE, "Connecting bridge %s (%s:%d)", context->bridge->name, context->bridge->addresses[context->bridge->cur_address].address, context->bridge->addresses[context->bridge->cur_address].port);
	rc = net__socket_connect(context,
			context->bridge->addresses[context->bridge->cur_address].address,
			context->bridge->addresses[context->bridge->cur_address].port,
			context->bridge->bind_address,
			false);

	if(rc > 0){
		if(rc == MOSQ_ERR_TLS){
			mux__delete(context);
			net__socket_close(context);
			return rc; /* Error already printed */
		}else if(rc == MOSQ_ERR_ERRNO){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", strerror(errno));
		}else if(rc == MOSQ_ERR_EAI){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", gai_strerror(errno));
		}

		return rc;
	}else if(rc == MOSQ_ERR_CONN_PENDING){
		mosquitto__set_state(context, mosq_cs_connect_pending);
		mux__add_out(context);
	}

	HASH_ADD(hh_sock, db.contexts_by_sock, sock, sizeof(context->sock), context);

	rc2 = send__connect(context, context->keepalive, context->clean_start, NULL);
	if(rc2 == MOSQ_ERR_SUCCESS){
		return rc;
	}else if(rc2 == MOSQ_ERR_ERRNO && errno == ENOTCONN){
		return MOSQ_ERR_SUCCESS;
	}else{
		if(rc2 == MOSQ_ERR_TLS){
			return rc2; /* Error already printed */
		}else if(rc2 == MOSQ_ERR_ERRNO){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", strerror(errno));
		}else if(rc2 == MOSQ_ERR_EAI){
			log__printf(NULL, MOSQ_LOG_ERR, "Error creating bridge: %s.", gai_strerror(errno));
		}
		mux__delete(context);
		net__socket_close(context);
		return rc2;
	}
}
#endif


int bridge__on_connect(struct mosquitto *context)
{
	int i;
	char *notification_topic;
	size_t notification_topic_len;
	char notification_payload;
	int sub_opts;
	bool retain = true;
	uint8_t qos;

	if(context->bridge->notifications){
		if(context->max_qos == 0){
			qos = 0;
		}else{
			qos = 1;
		}
		if(!context->retain_available){
			retain = false;
		}
		notification_payload = '1';
		if(context->bridge->notification_topic){
			if(!context->bridge->notifications_local_only){
				if(send__real_publish(context, mosquitto__mid_generate(context),
						context->bridge->notification_topic, 1, &notification_payload, qos, retain, 0, NULL, NULL, 0)){

					return 1;
				}
			}
			db__messages_easy_queue(context, context->bridge->notification_topic, qos, 1, &notification_payload, 1, 0, NULL);
		}else{
			notification_topic_len = strlen(context->bridge->remote_clientid)+strlen("$SYS/broker/connection//state");
			notification_topic = mosquitto__malloc(sizeof(char)*(notification_topic_len+1));
			if(!notification_topic) return MOSQ_ERR_NOMEM;

			snprintf(notification_topic, notification_topic_len+1, "$SYS/broker/connection/%s/state", context->bridge->remote_clientid);
			notification_payload = '1';
			if(!context->bridge->notifications_local_only){
				if(send__real_publish(context, mosquitto__mid_generate(context),
						notification_topic, 1, &notification_payload, qos, retain, 0, NULL, NULL, 0)){

					mosquitto__free(notification_topic);
					return 1;
				}
			}
			db__messages_easy_queue(context, notification_topic, qos, 1, &notification_payload, 1, 0, NULL);
			mosquitto__free(notification_topic);
		}
	}
	for(i=0; i<context->bridge->topic_count; i++){
		if(context->bridge->topics[i].direction == bd_in || context->bridge->topics[i].direction == bd_both){
			if(context->bridge->topics[i].qos > context->max_qos){
				sub_opts = context->max_qos;
			}else{
				sub_opts = context->bridge->topics[i].qos;
			}
			if(context->bridge->protocol_version == mosq_p_mqtt5){
				sub_opts = sub_opts
					| MQTT_SUB_OPT_NO_LOCAL
					| MQTT_SUB_OPT_RETAIN_AS_PUBLISHED
					| MQTT_SUB_OPT_SEND_RETAIN_ALWAYS;
			}
			if(send__subscribe(context, NULL, 1, &context->bridge->topics[i].remote_topic, sub_opts, NULL)){
				return 1;
			}
		}else{
			if(context->bridge->attempt_unsubscribe){
				if(send__unsubscribe(context, NULL, 1, &context->bridge->topics[i].remote_topic, NULL)){
					/* direction = inwards only. This means we should not be subscribed
					* to the topic. It is possible that we used to be subscribed to
					* this topic so unsubscribe. */
					return 1;
				}
			}
		}
	}
	for(i=0; i<context->bridge->topic_count; i++){
		if(context->bridge->topics[i].direction == bd_out || context->bridge->topics[i].direction == bd_both){
			if(context->bridge->topics[i].qos > context->max_qos){
				qos = context->max_qos;
			}else{
				qos = context->bridge->topics[i].qos;
			}
			retain__queue(context,
					context->bridge->topics[i].local_topic,
					qos, 0);
		}
	}

	bridge__backoff_reset(context);

	return MOSQ_ERR_SUCCESS;
}


int bridge__register_local_connections(void)
{
	struct mosquitto *context, *ctxt_tmp = NULL;

	HASH_ITER(hh_sock, db.contexts_by_sock, context, ctxt_tmp){
		if(context->bridge){
			if(mux__add_in(context)){
				log__printf(NULL, MOSQ_LOG_ERR, "Error in initial bridge registration: %s", strerror(errno));
				return MOSQ_ERR_UNKNOWN;
			}
			mux__add_out(context);
		}
	}
	return MOSQ_ERR_SUCCESS;
}


void bridge__cleanup(struct mosquitto *context)
{
	int i;

	for(i=0; i<db.bridge_count; i++){
		if(db.bridges[i] == context){
			db.bridges[i] = NULL;
		}
	}
	mosquitto__free(context->bridge->local_clientid);
	context->bridge->local_clientid = NULL;

	mosquitto__free(context->bridge->local_username);
	context->bridge->local_username = NULL;

	mosquitto__free(context->bridge->local_password);
	context->bridge->local_password = NULL;

	if(context->bridge->remote_clientid != context->id){
		mosquitto__free(context->bridge->remote_clientid);
	}
	context->bridge->remote_clientid = NULL;

	if(context->bridge->remote_username != context->username){
		mosquitto__free(context->bridge->remote_username);
	}
	context->bridge->remote_username = NULL;

	if(context->bridge->remote_password != context->password){
		mosquitto__free(context->bridge->remote_password);
	}
	context->bridge->remote_password = NULL;
#ifdef WITH_TLS
	if(context->ssl_ctx){
		SSL_CTX_free(context->ssl_ctx);
		context->ssl_ctx = NULL;
	}
#endif
}


void bridge__packet_cleanup(struct mosquitto *context)
{
	struct mosquitto__packet *packet;
	if(!context) return;

	if(context->current_out_packet){
		packet__cleanup(context->current_out_packet);
		mosquitto__free(context->current_out_packet);
		context->current_out_packet = NULL;
	}
	while(context->out_packet){
		packet__cleanup(context->out_packet);
		packet = context->out_packet;
		context->out_packet = context->out_packet->next;
		mosquitto__free(packet);
	}
	context->out_packet = NULL;
	context->out_packet_last = NULL;
	context->out_packet_count = 0;

	packet__cleanup(&(context->in_packet));
}

static int rand_between(int low, int high)
{
	int r;
	util__random_bytes(&r, sizeof(int));
	return (abs(r) % (high - low)) + low;
}

static void bridge__backoff_step(struct mosquitto *context)
{
	struct mosquitto__bridge *bridge;
	if(!context || !context->bridge) return;

	bridge = context->bridge;

	/* skip if not using backoff */
	if(bridge->backoff_cap){
		/* “Decorrelated Jitter” calculation, according to:
		 * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
		 */
		bridge->restart_timeout = rand_between(bridge->backoff_base, bridge->restart_timeout * 3);
		if(bridge->restart_timeout > bridge->backoff_cap){
			bridge->restart_timeout = bridge->backoff_cap;
		}
	}
}

static void bridge__backoff_reset(struct mosquitto *context)
{
	struct mosquitto__bridge *bridge;
	if(!context || !context->bridge) return;

	bridge = context->bridge;

	/* skip if not using backoff */
	if(bridge->backoff_cap){
		bridge->restart_timeout = bridge->backoff_base;
	}
}


static void bridge_check_pending(struct mosquitto *context)
{
	int err;
	socklen_t len;

	if(context->state == mosq_cs_connect_pending){
		len = sizeof(int);
		if(!getsockopt(context->sock, SOL_SOCKET, SO_ERROR, (char *)&err, &len)){
			if(err == 0){
				mosquitto__set_state(context, mosq_cs_new);
#if defined(WITH_ADNS) && defined(WITH_BRIDGE)
				if(context->bridge){
					bridge__connect_step3(context);
				}
#endif
			}else if(err == ECONNREFUSED){
				do_disconnect(context, MOSQ_ERR_CONN_LOST);
				return;
			}
		}else{
			do_disconnect(context, MOSQ_ERR_CONN_LOST);
			return;
		}
	}
}

void bridge_check(void)
{
	static time_t last_check = 0;
	struct mosquitto *context = NULL;
	socklen_t len;
	int i;
	int rc;
	int err;

	if(db.now_s <= last_check) return;

	for(i=0; i<db.bridge_count; i++){
		if(!db.bridges[i]) continue;

		context = db.bridges[i];

		if(context->sock != INVALID_SOCKET){
			mosquitto__check_keepalive(context);
			bridge_check_pending(context);

			/* Check for bridges that are not round robin and not currently
			 * connected to their primary broker. */
			if(context->bridge->round_robin == false
					&& context->bridge->cur_address != 0
					&& context->bridge->primary_retry
					&& db.now_s > context->bridge->primary_retry){

				if(context->bridge->primary_retry_sock == INVALID_SOCKET){
					rc = net__try_connect(context->bridge->addresses[0].address,
							context->bridge->addresses[0].port,
							&context->bridge->primary_retry_sock,
							context->bridge->bind_address, false);

					if(rc == 0){
						COMPAT_CLOSE(context->bridge->primary_retry_sock);
						context->bridge->primary_retry_sock = INVALID_SOCKET;
						context->bridge->primary_retry = 0;
						mux__delete(context);
						net__socket_close(context);
						context->bridge->cur_address = 0;
					}
				}else{
					len = sizeof(int);
					if(!getsockopt(context->bridge->primary_retry_sock, SOL_SOCKET, SO_ERROR, (char *)&err, &len)){
						if(err == 0){
							COMPAT_CLOSE(context->bridge->primary_retry_sock);
							context->bridge->primary_retry_sock = INVALID_SOCKET;
							context->bridge->primary_retry = 0;
							mux__delete(context);
							net__socket_close(context);
							context->bridge->cur_address = context->bridge->address_count-1;
						}else{
							COMPAT_CLOSE(context->bridge->primary_retry_sock);
							context->bridge->primary_retry_sock = INVALID_SOCKET;
							context->bridge->primary_retry = db.now_s+5;
						}
					}else{
						COMPAT_CLOSE(context->bridge->primary_retry_sock);
						context->bridge->primary_retry_sock = INVALID_SOCKET;
						context->bridge->primary_retry = db.now_s+5;
					}
				}
			}
		}



		if(context->sock == INVALID_SOCKET){
			/* Want to try to restart the bridge connection */
			if(!context->bridge->restart_t){
				context->bridge->restart_t = db.now_s+context->bridge->restart_timeout;
				context->bridge->cur_address++;
				if(context->bridge->cur_address == context->bridge->address_count){
					context->bridge->cur_address = 0;
				}
			}else{
				if((context->bridge->start_type == bst_lazy && context->bridge->lazy_reconnect)
						|| (context->bridge->start_type == bst_automatic && db.now_s > context->bridge->restart_t)){

#if defined(__GLIBC__) && defined(WITH_ADNS)
					if(context->adns){
						/* Connection attempted, waiting on DNS lookup */
						rc = gai_error(context->adns);
						if(rc == EAI_INPROGRESS){
							/* Just keep on waiting */
						}else if(rc == 0){
							rc = bridge__connect_step2(context);
							if(rc == MOSQ_ERR_SUCCESS){
								mux__add_in(context);
								if(context->current_out_packet){
									mux__add_out(context);
								}
							}else if(rc == MOSQ_ERR_CONN_PENDING){
								mux__add_in(context);
								mux__add_out(context);
								context->bridge->restart_t = 0;
							}else{
								context->bridge->cur_address++;
								if(context->bridge->cur_address == context->bridge->address_count){
									context->bridge->cur_address = 0;
								}
								context->bridge->restart_t = 0;
							}
						}else{
							/* Need to retry */
							if(context->adns->ar_result){
								freeaddrinfo(context->adns->ar_result);
							}
							mosquitto__free(context->adns);
							context->adns = NULL;
							context->bridge->restart_t = 0;
						}
					}else{
						rc = bridge__connect_step1(context);
						if(rc){
							context->bridge->cur_address++;
							if(context->bridge->cur_address == context->bridge->address_count){
								context->bridge->cur_address = 0;
							}
						}else{
							/* Short wait for ADNS lookup */
							context->bridge->restart_t = 1;
						}
					}
#else
					{
						rc = bridge__connect(context);
						context->bridge->restart_t = 0;
						if(rc == MOSQ_ERR_SUCCESS || rc == MOSQ_ERR_CONN_PENDING){
							if(context->bridge->round_robin == false && context->bridge->cur_address != 0){
								context->bridge->primary_retry = db.now_s + 5;
							}
							mux__add_in(context);
							if(context->current_out_packet){
								mux__add_out(context);
							}
						}else{
							context->bridge->cur_address++;
							if(context->bridge->cur_address == context->bridge->address_count){
								context->bridge->cur_address = 0;
							}
						}
					}
#endif
				}
			}
		}
	}
}

#endif
