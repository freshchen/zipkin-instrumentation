package com.github.freshchen.jedis263.instrumentation;

import brave.Span;
import com.github.freshchen.instrumentation.core.util.TagUtils;
import com.github.freshchen.jedis.instrumentation.util.JedisTracerHelper;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.util.Pool;
import redis.clients.util.Slowlog;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author freshchen
 * @since 2022/2/27
 */
public class TraceableJedis263 extends Jedis {

    private final Jedis delegate;
    private final JedisTracerHelper helper;

    public TraceableJedis263(Jedis delegate, JedisTracerHelper tracerHelper) {
        super(delegate.getClient().getHost());
        this.delegate = delegate;
        this.helper = tracerHelper;
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        Span span = helper.startNextJedisSpan("append", key);
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.append(key, value));
    }

    @Override
    public Long append(String key, String value) {
        Span span = helper.startNextJedisSpan("append", key);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.append(key, value));
    }

    @Override
    public String asking() {
        Span span = helper.startNextJedisSpan("asking");
        return helper.executeInScope(span, () -> delegate.asking());
    }

    @Override
    public String auth(String password) {
        Span span = helper.startNextJedisSpan("auth");
        return helper.executeInScope(span, () -> delegate.auth(password));
    }

    @Override
    public String bgrewriteaof() {
        Span span = helper.startNextJedisSpan("bgrewriteaof");
        return helper.executeInScope(span, () -> delegate.bgrewriteaof());
    }

    @Override
    public String bgsave() {
        Span span = helper.startNextJedisSpan("bgsave");
        return helper.executeInScope(span, delegate::bgsave);
    }

    @Override
    public Long bitcount(byte[] key) {
        Span span = helper.startNextJedisSpan("bitcount", key);
        return helper.executeInScope(span, () -> delegate.bitcount(key));
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("bitcount", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.bitcount(key, start, end));
    }

    @Override
    public Long bitcount(String key) {
        Span span = helper.startNextJedisSpan("bitcount", key);
        return helper.executeInScope(span, () -> delegate.bitcount(key));
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("bitcount", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.bitcount(key, start, end));
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        Span span = helper.startNextJedisSpan("bitop");
        span.tag("destKey", Arrays.toString(destKey));
        span.tag("srcKeys", TagUtils.toString(srcKeys));
        return helper.executeInScope(span, () -> delegate.bitop(op, destKey, srcKeys));
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        Span span = helper.startNextJedisSpan("bitop");
        span.tag("destKey", destKey);
        span.tag("srcKeys", Arrays.toString(srcKeys));
        return helper.executeInScope(span, () -> delegate.bitop(op, destKey, srcKeys));
    }

    @Override
    public Long bitpos(byte[] key, boolean value) {
        Span span = helper.startNextJedisSpan("bitpos", key);
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.bitpos(key, value));
    }

    @Override
    public Long bitpos(byte[] key, boolean value, BitPosParams params) {
        Span span = helper.startNextJedisSpan("bitpos", key);
        span.tag("value", String.valueOf(value));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.bitpos(key, value, params));
    }

    @Override
    public Long bitpos(String key, boolean value) {
        Span span = helper.startNextJedisSpan("bitpos", key);
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.bitpos(key, value));
    }

    @Override
    public Long bitpos(String key, boolean value, BitPosParams params) {
        Span span = helper.startNextJedisSpan("bitpos", key);
        span.tag("value", String.valueOf(value));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.bitpos(key, value, params));
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        Span span = helper.startNextJedisSpan("blpop");
        span.tag("timeout", String.valueOf(timeout));
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.blpop(timeout, keys));
    }

    @Override
    public List<byte[]> blpop(byte[] arg) {
        Span span = helper.startNextJedisSpan("blpop");
        span.tag("arg", Arrays.toString(arg));
        return helper.executeInScope(span, () -> delegate.blpop(arg));
    }

    @Override
    public List<byte[]> blpop(byte[]... args) {
        Span span = helper.startNextJedisSpan("blpop");
        span.tag("args", TagUtils.toString(args));
        return helper.executeInScope(span, () -> delegate.blpop(args));
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        Span span = helper.startNextJedisSpan("blpop", keys);
        span.tag("timeout", String.valueOf(timeout));
        return helper.executeInScope(span, () -> delegate.blpop(timeout, keys));
    }

    @Override
    public List<String> blpop(String... args) {
        Span span = helper.startNextJedisSpan("blpop");
        span.tag("args", Arrays.toString(args));
        return helper.executeInScope(span, () -> delegate.blpop(args));
    }

    @Override
    public List<String> blpop(String arg) {
        Span span = helper.startNextJedisSpan("blpop");
        span.tag("arg", arg);
        return helper.executeInScope(span, () -> delegate.blpop(arg));
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        Span span = helper.startNextJedisSpan("blpop");
        span.tag("timeout", String.valueOf(timeout));
        return helper.executeInScope(span, () -> delegate.blpop(timeout, key));
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        Span span = helper.startNextJedisSpan("brpop");
        span.tag("timeout", String.valueOf(timeout));
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.brpop(timeout, keys));
    }

    @Override
    public List<byte[]> brpop(byte[] arg) {
        Span span = helper.startNextJedisSpan("brpop");
        span.tag("arg", Arrays.toString(arg));
        return helper.executeInScope(span, () -> delegate.brpop(arg));
    }

    @Override
    public List<byte[]> brpop(byte[]... args) {
        Span span = helper.startNextJedisSpan("brpop");
        span.tag("args", TagUtils.toString(args));
        return helper.executeInScope(span, () -> delegate.brpop(args));
    }

    @Override
    public List<String> brpop(String... args) {
        Span span = helper.startNextJedisSpan("brpop");
        span.tag("args", Arrays.toString(args));
        return helper.executeInScope(span, () -> delegate.brpop(args));
    }

    @Override
    public List<String> brpop(String arg) {
        Span span = helper.startNextJedisSpan("brpop");
        span.tag("arg", arg);
        return helper.executeInScope(span, () -> delegate.brpop(arg));
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        Span span = helper.startNextJedisSpan("brpop", keys);
        span.tag("timeout", String.valueOf(timeout));
        return helper.executeInScope(span, () -> delegate.brpop(timeout, keys));
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        Span span = helper.startNextJedisSpan("brpop");
        span.tag("timeout", String.valueOf(timeout));
        return helper.executeInScope(span, () -> delegate.brpop(timeout, key));
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        Span span = helper.startNextJedisSpan("brpoplpush");
        span.tag("timeout", String.valueOf(timeout));
        span.tag("source", Arrays.toString(source));
        span.tag("destination", Arrays.toString(destination));
        return helper.executeInScope(span, () -> delegate.brpoplpush(source, destination, timeout));
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        Span span = helper.startNextJedisSpan("brpoplpush");
        span.tag("source", source);
        span.tag("destination", destination);
        span.tag("timeout", String.valueOf(timeout));
        return helper.executeInScope(span, () -> delegate.brpoplpush(source, destination, timeout));
    }

    @Override
    public String clientGetname() {
        Span span = helper.startNextJedisSpan("clientGetname");
        return helper.executeInScope(span, () -> delegate.clientGetname());
    }

    @Override
    public String clientKill(byte[] client) {
        Span span = helper.startNextJedisSpan("clientKill");
        span.tag("client", Arrays.toString(client));
        return helper.executeInScope(span, () -> delegate.clientKill(client));
    }

    @Override
    public String clientKill(String client) {
        Span span = helper.startNextJedisSpan("clientKill");
        span.tag("client", client);
        return helper.executeInScope(span, () -> delegate.clientKill(client));
    }

    @Override
    public String clientList() {
        Span span = helper.startNextJedisSpan("clientList");
        return helper.executeInScope(span, () -> delegate.clientList());
    }

    @Override
    public String clientSetname(byte[] name) {
        Span span = helper.startNextJedisSpan("clientSetname");
        span.tag("name", Arrays.toString(name));
        return helper.executeInScope(span, () -> delegate.clientSetname(name));
    }

    @Override
    public String clientSetname(String name) {
        Span span = helper.startNextJedisSpan("clientSetname");
        span.tag("name", name);
        return helper.executeInScope(span, () -> delegate.clientSetname(name));
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String clusterAddSlots(int... slots) {
        Span span = helper.startNextJedisSpan("clusterAddSlots");
        span.tag("slots", Arrays.toString(slots));
        return helper.executeInScope(span, () -> delegate.clusterAddSlots(slots));
    }

    @Override
    public Long clusterCountKeysInSlot(int slot) {
        Span span = helper.startNextJedisSpan("clusterCountKeysInSlot");
        span.tag("slot", String.valueOf(slot));
        return helper.executeInScope(span, () -> delegate.clusterCountKeysInSlot(slot));
    }

    @Override
    public String clusterDelSlots(int... slots) {
        Span span = helper.startNextJedisSpan("clusterDelSlots");
        span.tag("slots", Arrays.toString(slots));
        return helper.executeInScope(span, () -> delegate.clusterDelSlots(slots));
    }

    @Override
    public String clusterFailover() {
        Span span = helper.startNextJedisSpan("clusterFailover");
        return helper.executeInScope(span, () -> delegate.clusterFailover());
    }

    @Override
    public String clusterFlushSlots() {
        Span span = helper.startNextJedisSpan("clusterFlushSlots");
        return helper.executeInScope(span, () -> delegate.clusterFlushSlots());
    }

    @Override
    public String clusterForget(String nodeId) {
        Span span = helper.startNextJedisSpan("clusterForget");
        span.tag("nodeId", nodeId);
        return helper.executeInScope(span, () -> delegate.clusterForget(nodeId));
    }

    @Override
    public List<String> clusterGetKeysInSlot(int slot, int count) {
        Span span = helper.startNextJedisSpan("clusterGetKeysInSlot");
        span.tag("slot", String.valueOf(slot));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.clusterGetKeysInSlot(slot, count));
    }

    @Override
    public String clusterInfo() {
        Span span = helper.startNextJedisSpan("clusterInfo");
        return helper.executeInScope(span, () -> delegate.clusterInfo());
    }

    @Override
    public Long clusterKeySlot(String key) {
        Span span = helper.startNextJedisSpan("clusterKeySlot", key);
        return helper.executeInScope(span, () -> delegate.clusterKeySlot(key));
    }

    @Override
    public String clusterMeet(String ip, int port) {
        Span span = helper.startNextJedisSpan("clusterMeet");
        span.tag("ip", ip);
        span.tag("port", String.valueOf(port));
        return helper.executeInScope(span, () -> delegate.clusterMeet(ip, port));
    }

    @Override
    public String clusterNodes() {
        Span span = helper.startNextJedisSpan("clusterNodes");
        return helper.executeInScope(span, () -> delegate.clusterNodes());
    }

    @Override
    public String clusterReplicate(String nodeId) {
        Span span = helper.startNextJedisSpan("clusterReplicate");
        span.tag("nodeId", nodeId);
        return helper.executeInScope(span, () -> delegate.clusterReplicate(nodeId));
    }

    @Override
    public String clusterReset(JedisCluster.Reset resetType) {
        Span span = helper.startNextJedisSpan("clusterReset");
        span.tag("resetType", resetType.name());
        return helper.executeInScope(span, () -> delegate.clusterReset(resetType));
    }

    @Override
    public String clusterSaveConfig() {
        Span span = helper.startNextJedisSpan("clusterSaveConfig");
        return helper.executeInScope(span, () -> delegate.clusterSaveConfig());
    }

    @Override
    public String clusterSetSlotImporting(int slot, String nodeId) {
        Span span = helper.startNextJedisSpan("clusterSetSlotImporting");
        span.tag("slot", String.valueOf(slot));
        span.tag("nodeId", nodeId);
        return helper.executeInScope(span, () -> delegate.clusterSetSlotImporting(slot, nodeId));
    }

    @Override
    public String clusterSetSlotMigrating(int slot, String nodeId) {
        Span span = helper.startNextJedisSpan("clusterSetSlotMigrating");
        span.tag("slot", String.valueOf(slot));
        span.tag("nodeId", nodeId);
        return helper.executeInScope(span, () -> delegate.clusterSetSlotMigrating(slot, nodeId));
    }

    @Override
    public String clusterSetSlotNode(int slot, String nodeId) {
        Span span = helper.startNextJedisSpan("clusterSetSlotNode");
        span.tag("slot", String.valueOf(slot));
        span.tag("nodeId", nodeId);
        return helper.executeInScope(span, () -> delegate.clusterSetSlotNode(slot, nodeId));
    }

    @Override
    public String clusterSetSlotStable(int slot) {
        Span span = helper.startNextJedisSpan("clusterSetSlotStable");
        span.tag("slot", String.valueOf(slot));
        return helper.executeInScope(span, () -> delegate.clusterSetSlotStable(slot));
    }

    @Override
    public List<String> clusterSlaves(String nodeId) {
        Span span = helper.startNextJedisSpan("clusterSlaves");
        span.tag("nodeId", nodeId);
        return helper.executeInScope(span, () -> delegate.clusterSlaves(nodeId));
    }

    @Override
    public List<Object> clusterSlots() {
        Span span = helper.startNextJedisSpan("clusterSlots");
        return helper.executeInScope(span, () -> delegate.clusterSlots());
    }

    @Override
    public List<byte[]> configGet(byte[] pattern) {
        Span span = helper.startNextJedisSpan("configGet");
        span.tag("pattern", Arrays.toString(pattern));
        return helper.executeInScope(span, () -> delegate.configGet(pattern));
    }

    @Override
    public List<String> configGet(String pattern) {
        Span span = helper.startNextJedisSpan("configGet");
        span.tag("pattern", pattern);
        return helper.executeInScope(span, () -> delegate.configGet(pattern));
    }

    @Override
    public String configResetStat() {
        Span span = helper.startNextJedisSpan("configResetStat");
        return helper.executeInScope(span, () -> delegate.configResetStat());
    }

    @Override
    public byte[] configSet(byte[] parameter, byte[] value) {
        Span span = helper.startNextJedisSpan("configSet");
        span.tag("parameter", Arrays.toString(parameter));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.configSet(parameter, value));
    }

    @Override
    public String configSet(String parameter, String value) {
        Span span = helper.startNextJedisSpan("configSet");
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.configSet(parameter, value));
    }

    @Override
    public void connect() {
        Span span = helper.startNextJedisSpan("connect");
        helper.executeInScope(span, () -> delegate.connect());
    }

    @Override
    public Long dbSize() {
        Span span = helper.startNextJedisSpan("dbSize");
        return helper.executeInScope(span, () -> delegate.dbSize());
    }

    @Override
    public String debug(DebugParams params) {
        Span span = helper.startNextJedisSpan("debug");
        span.tag("params", Arrays.toString(params.getCommand()));
        return helper.executeInScope(span, () -> delegate.debug(params));
    }

    @Override
    public Long decr(byte[] key) {
        Span span = helper.startNextJedisSpan("decr", key);
        return helper.executeInScope(span, () -> delegate.decr(key));
    }

    @Override
    public Long decr(String key) {
        Span span = helper.startNextJedisSpan("decr", key);
        return helper.executeInScope(span, () -> delegate.decr(key));
    }

    @Override
    public Long decrBy(byte[] key, long integer) {
        Span span = helper.startNextJedisSpan("decrBy", key);
        span.tag("integer", String.valueOf(integer));
        return helper.executeInScope(span, () -> delegate.decrBy(key, integer));
    }

    @Override
    public Long decrBy(String key, long integer) {
        Span span = helper.startNextJedisSpan("decrBy", key);
        span.tag("integer", String.valueOf(integer));
        return helper.executeInScope(span, () -> delegate.decrBy(key, integer));
    }

    @Override
    public Long del(byte[]... keys) {
        Span span = helper.startNextJedisSpan("del");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.del(keys));
    }

    @Override
    public Long del(byte[] key) {
        Span span = helper.startNextJedisSpan("del", key);
        return helper.executeInScope(span, () -> delegate.del(key));
    }

    @Override
    public Long del(String... keys) {
        Span span = helper.startNextJedisSpan("del", keys);
        return helper.executeInScope(span, () -> delegate.del(keys));
    }

    @Override
    public Long del(String key) {
        Span span = helper.startNextJedisSpan("del", key);
        return helper.executeInScope(span, () -> delegate.del(key));
    }

    @Override
    public void disconnect() {
        Span span = helper.startNextJedisSpan("disconnect");
        helper.executeInScope(span, () -> delegate.disconnect());
    }

    @Override
    public byte[] dump(byte[] key) {
        Span span = helper.startNextJedisSpan("dump", key);
        return helper.executeInScope(span, () -> delegate.dump(key));
    }

    @Override
    public byte[] dump(String key) {
        Span span = helper.startNextJedisSpan("dump", key);
        return helper.executeInScope(span, () -> delegate.dump(key));
    }

    @Override
    public byte[] echo(byte[] string) {
        Span span = helper.startNextJedisSpan("echo");
        span.tag("string", Arrays.toString(string));
        return helper.executeInScope(span, () -> delegate.echo(string));
    }

    @Override
    public String echo(String string) {
        Span span = helper.startNextJedisSpan("echo");
        span.tag("string", string);
        return helper.executeInScope(span, () -> delegate.echo(string));
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("script", Arrays.toString(script));
        span.tag("keys", TagUtils.toString(keys));
        span.tag("args", TagUtils.toString(args));
        return helper.executeInScope(span, () -> delegate.eval(script, keys, args));
    }

    @Override
    public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("script", Arrays.toString(script));
        span.tag("keyCount", Arrays.toString(keyCount));
        span.tag("params", TagUtils.toString(params));
        return helper.executeInScope(span, () -> delegate.eval(script, keyCount, params));
    }

    @Override
    public Object eval(byte[] script, int keyCount, byte[]... params) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("script", Arrays.toString(script));
        span.tag("keyCount", String.valueOf(keyCount));
        span.tag("params", TagUtils.toString(params));
        return helper.executeInScope(span, () -> delegate.eval(script, keyCount, params));
    }

    @Override
    public Object eval(byte[] script) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("script", Arrays.toString(script));
        return helper.executeInScope(span, () -> delegate.eval(script));
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("keyCount", String.valueOf(keyCount));
        span.tag("params", Arrays.toString(params));
        return helper.executeInScope(span, () -> delegate.eval(script, keyCount, params));
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("keys", TagUtils.toString(keys));
        span.tag("args", TagUtils.toString(args));
        return helper.executeInScope(span, () -> delegate.eval(script, keys, args));
    }

    @Override
    public Object eval(String script) {
        Span span = helper.startNextJedisSpan("eval");
        span.tag("script", script);
        return helper.executeInScope(span, () -> delegate.eval(script));
    }

    @Override
    public Object evalsha(byte[] sha1) {
        Span span = helper.startNextJedisSpan("evalsha");
        span.tag("sha1", Arrays.toString(sha1));
        return helper.executeInScope(span, () -> delegate.evalsha(sha1));
    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        Span span = helper.startNextJedisSpan("evalsha");
        span.tag("sha1", Arrays.toString(sha1));
        span.tag("keys", TagUtils.toString(keys));
        span.tag("args", TagUtils.toString(args));
        return helper.executeInScope(span, () -> delegate.evalsha(sha1, keys, args));
    }

    @Override
    public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
        Span span = helper.startNextJedisSpan("evalsha");
        span.tag("params", TagUtils.toString(params));
        span.tag("sha1", Arrays.toString(sha1));
        span.tag("keyCount", String.valueOf(keyCount));
        return helper.executeInScope(span, () -> delegate.evalsha(sha1, keyCount, params));
    }

    @Override
    public Object evalsha(String script) {
        Span span = helper.startNextJedisSpan("evalsha");
        span.tag("script", script);
        return helper.executeInScope(span, () -> delegate.evalsha(script));
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        Span span = helper.startNextJedisSpan("evalsha");
        span.tag("keys", TagUtils.toString(keys));
        span.tag("args", TagUtils.toString(args));
        span.tag("sha1", sha1);
        return helper.executeInScope(span, () -> delegate.evalsha(sha1, keys, args));
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        Span span = helper.startNextJedisSpan("evalsha");
        span.tag("keyCount", String.valueOf(keyCount));
        span.tag("params", Arrays.toString(params));
        span.tag("sha1", sha1);
        return helper.executeInScope(span, () -> delegate.evalsha(sha1, keyCount, params));
    }

    @Override
    public Boolean exists(byte[] key) {
        Span span = helper.startNextJedisSpan("exists", key);
        return helper.executeInScope(span, () -> delegate.exists(key));
    }

    @Override
    public Boolean exists(String key) {
        Span span = helper.startNextJedisSpan("exists", key);
        return helper.executeInScope(span, () -> delegate.exists(key));
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        Span span = helper.startNextJedisSpan("expire", key);
        span.tag("seconds", String.valueOf(seconds));
        return helper.executeInScope(span, () -> delegate.expire(key, seconds));
    }

    @Override
    public Long expire(String key, int seconds) {
        Span span = helper.startNextJedisSpan("expire", key);
        span.tag("seconds", String.valueOf(seconds));
        return helper.executeInScope(span, () -> delegate.expire(key, seconds));
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {
        Span span = helper.startNextJedisSpan("expireAt", key);
        span.tag("unixTime", String.valueOf(unixTime));
        return helper.executeInScope(span, () -> delegate.expireAt(key, unixTime));
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        Span span = helper.startNextJedisSpan("expireAt", key);
        span.tag("unixTime", String.valueOf(unixTime));
        return helper.executeInScope(span, () -> delegate.expireAt(key, unixTime));
    }

    @Override
    public String flushAll() {
        Span span = helper.startNextJedisSpan("flushAll");
        return helper.executeInScope(span, () -> delegate.flushAll());
    }

    @Override
    public String flushDB() {
        Span span = helper.startNextJedisSpan("flushDB");
        return helper.executeInScope(span, () -> delegate.flushDB());
    }

    @Override
    public byte[] get(byte[] key) {
        Span span = helper.startNextJedisSpan("get", key);
        return helper.executeInScope(span, () -> delegate.get(key));
    }

    @Override
    public String get(String key) {
        Span span = helper.startNextJedisSpan("get", key);
        return helper.executeInScope(span, () -> delegate.get(key));
    }

    @Override
    public Client getClient() {
        Span span = helper.startNextJedisSpan("getClient");
        return helper.executeInScope(span, () -> delegate.getClient());
    }

    @Override
    public Long getDB() {
        Span span = helper.startNextJedisSpan("getDB");
        return helper.executeInScope(span, () -> {
            Long db = delegate.getDB();
            span.tag("db", String.valueOf(db));
            return db;
        });
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        Span span = helper.startNextJedisSpan("getSet", key);
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.getSet(key, value));
    }

    @Override
    public String getSet(String key, String value) {
        Span span = helper.startNextJedisSpan("getSet", key);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.getSet(key, value));
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        Span span = helper.startNextJedisSpan("getbit", key);
        span.tag("offset", String.valueOf(offset));
        return helper.executeInScope(span, () -> delegate.getbit(key, offset));
    }

    @Override
    public Boolean getbit(String key, long offset) {
        Span span = helper.startNextJedisSpan("getbit", key);
        span.tag("offset", String.valueOf(offset));
        return helper.executeInScope(span, () -> delegate.getbit(key, offset));
    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        Span span = helper.startNextJedisSpan("getrange", key);
        span.tag("startOffset", String.valueOf(startOffset));
        span.tag("endOffset", String.valueOf(endOffset));
        return helper.executeInScope(span, () -> delegate.getrange(key, startOffset, endOffset));
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        Span span = helper.startNextJedisSpan("getrange", key);
        span.tag("startOffset", String.valueOf(startOffset));
        span.tag("endOffset", String.valueOf(endOffset));
        return helper.executeInScope(span, () -> delegate.getrange(key, startOffset, endOffset));
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        Span span = helper.startNextJedisSpan("hdel", key);
        span.tag("fields", TagUtils.toString(fields));
        return helper.executeInScope(span, () -> delegate.hdel(key, fields));
    }

    @Override
    public Long hdel(String key, String... fields) {
        Span span = helper.startNextJedisSpan("hdel", key);
        span.tag("fields", Arrays.toString(fields));
        return helper.executeInScope(span, () -> delegate.hdel(key, fields));
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        Span span = helper.startNextJedisSpan("hexists", key);
        span.tag("field", Arrays.toString(field));
        return helper.executeInScope(span, () -> delegate.hexists(key, field));
    }

    @Override
    public Boolean hexists(String key, String field) {
        Span span = helper.startNextJedisSpan("hexists", key);
        span.tag("field", field);
        return helper.executeInScope(span, () -> delegate.hexists(key, field));
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        Span span = helper.startNextJedisSpan("hget", key);
        span.tag("field", Arrays.toString(field));
        return helper.executeInScope(span, () -> delegate.hget(key, field));
    }

    @Override
    public String hget(String key, String field) {
        Span span = helper.startNextJedisSpan("hget", key);
        span.tag("field", field);
        return helper.executeInScope(span, () -> delegate.hget(key, field));
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Span span = helper.startNextJedisSpan("hgetAll", key);
        return helper.executeInScope(span, () -> delegate.hgetAll(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Span span = helper.startNextJedisSpan("hgetAll", key);
        return helper.executeInScope(span, () -> delegate.hgetAll(key));
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        Span span = helper.startNextJedisSpan("hincrBy", key);
        span.tag("field", Arrays.toString(field));
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.hincrBy(key, field, value));
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        Span span = helper.startNextJedisSpan("hincrBy", key);
        span.tag("field", field);
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.hincrBy(key, field, value));
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        Span span = helper.startNextJedisSpan("hincrByFloat", key);
        span.tag("field", Arrays.toString(field));
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.hincrByFloat(key, field, value));
    }

    @Override
    public Double hincrByFloat(String key, String field, double value) {
        Span span = helper.startNextJedisSpan("hincrByFloat", key);
        span.tag("field", field);
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.hincrByFloat(key, field, value));
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        Span span = helper.startNextJedisSpan("hkeys", key);
        return helper.executeInScope(span, () -> delegate.hkeys(key));
    }

    @Override
    public Set<String> hkeys(String key) {
        Span span = helper.startNextJedisSpan("hkeys", key);
        return helper.executeInScope(span, () -> delegate.hkeys(key));
    }

    @Override
    public Long hlen(byte[] key) {
        Span span = helper.startNextJedisSpan("hlen", key);
        return helper.executeInScope(span, () -> delegate.hlen(key));
    }

    @Override
    public Long hlen(String key) {
        Span span = helper.startNextJedisSpan("hlen", key);
        return helper.executeInScope(span, () -> delegate.hlen(key));
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        Span span = helper.startNextJedisSpan("hmget", key);
        span.tag("fields", TagUtils.toString(fields));
        return helper.executeInScope(span, () -> delegate.hmget(key, fields));
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        Span span = helper.startNextJedisSpan("hmget", key);
        span.tag("fields", Arrays.toString(fields));
        return helper.executeInScope(span, () -> delegate.hmget(key, fields));
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        Span span = helper.startNextJedisSpan("hmset", key);
        span.tag("hash", TagUtils.toStringMap(hash));
        return helper.executeInScope(span, () -> delegate.hmset(key, hash));
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        Span span = helper.startNextJedisSpan("hmset", key);
        span.tag("hash", TagUtils.toString(hash));
        return helper.executeInScope(span, () -> delegate.hmset(key, hash));
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
        Span span = helper.startNextJedisSpan("hscan", key);
        span.tag("cursor", Arrays.toString(cursor));
        return helper.executeInScope(span, () -> delegate.hscan(key, cursor));
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("hscan", key);
        span.tag("cursor", Arrays.toString(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.hscan(key, cursor, params));
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, int cursor) {
        Span span = helper.startNextJedisSpan("hscan", key);
        span.tag("cursor", String.valueOf(cursor));
        return helper.executeInScope(span, () -> delegate.hscan(key, cursor));
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, int cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("hscan", key);
        span.tag("cursor", String.valueOf(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.hscan(key, cursor, params));
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        Span span = helper.startNextJedisSpan("hscan", key);
        span.tag("cursor", cursor);
        return helper.executeInScope(span, () -> delegate.hscan(key, cursor));
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("hscan", key);
        span.tag("cursor", cursor);
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.hscan(key, cursor, params));
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        Span span = helper.startNextJedisSpan("hset", key);
        span.tag("field", Arrays.toString(field));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.hset(key, field, value));
    }

    @Override
    public Long hset(String key, String field, String value) {
        Span span = helper.startNextJedisSpan("hset", key);
        span.tag("field", field);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.hset(key, field, value));
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        Span span = helper.startNextJedisSpan("hsetnx", key);
        span.tag("field", Arrays.toString(field));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.hsetnx(key, field, value));
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        Span span = helper.startNextJedisSpan("hsetnx", key);
        span.tag("field", field);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.hsetnx(key, field, value));
    }

    @Override
    public List<byte[]> hvals(byte[] key) {
        Span span = helper.startNextJedisSpan("hvals", key);
        return helper.executeInScope(span, () -> delegate.hvals(key));
    }

    @Override
    public List<String> hvals(String key) {
        Span span = helper.startNextJedisSpan("hvals", key);
        return helper.executeInScope(span, () -> delegate.hvals(key));
    }

    @Override
    public Long incr(byte[] key) {
        Span span = helper.startNextJedisSpan("incr", key);
        return helper.executeInScope(span, () -> delegate.incr(key));
    }

    @Override
    public Long incr(String key) {
        Span span = helper.startNextJedisSpan("incr", key);
        return helper.executeInScope(span, () -> delegate.incr(key));
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        Span span = helper.startNextJedisSpan("incrBy", key);
        span.tag("integer", String.valueOf(integer));
        return helper.executeInScope(span, () -> delegate.incrBy(key, integer));
    }

    @Override
    public Long incrBy(String key, long integer) {
        Span span = helper.startNextJedisSpan("incrBy", key);
        span.tag("integer", String.valueOf(integer));
        return helper.executeInScope(span, () -> delegate.incrBy(key, integer));
    }

    @Override
    public Double incrByFloat(byte[] key, double integer) {
        Span span = helper.startNextJedisSpan("incrByFloat", key);
        span.tag("integer", String.valueOf(integer));
        return helper.executeInScope(span, () -> delegate.incrByFloat(key, integer));
    }

    @Override
    public Double incrByFloat(String key, double value) {
        Span span = helper.startNextJedisSpan("incrByFloat", key);
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.incrByFloat(key, value));
    }

    @Override
    public String info() {
        Span span = helper.startNextJedisSpan("info");
        return helper.executeInScope(span, () -> delegate.info());
    }

    @Override
    public String info(String section) {
        Span span = helper.startNextJedisSpan("info");
        span.tag("section", section);
        return helper.executeInScope(span, () -> delegate.info(section));
    }

    @Override
    public boolean isConnected() {
        Span span = helper.startNextJedisSpan("isConnected");
        return helper.executeInScope(span, () -> delegate.isConnected());
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Span span = helper.startNextJedisSpan("keys");
        span.tag("pattern", Arrays.toString(pattern));
        return helper.executeInScope(span, () -> delegate.keys(pattern));
    }

    @Override
    public Set<String> keys(String pattern) {
        Span span = helper.startNextJedisSpan("keys");
        span.tag("pattern", Objects.toString(pattern));
        return helper.executeInScope(span, () -> delegate.keys(pattern));
    }

    @Override
    public Long lastsave() {
        Span span = helper.startNextJedisSpan("lastsave");
        return helper.executeInScope(span, () -> delegate.lastsave());
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        Span span = helper.startNextJedisSpan("lindex", key);
        span.tag("index", String.valueOf(index));
        return helper.executeInScope(span, () -> delegate.lindex(key, index));
    }

    @Override
    public String lindex(String key, long index) {
        Span span = helper.startNextJedisSpan("lindex", key);
        span.tag("index", String.valueOf(index));
        return helper.executeInScope(span, () -> delegate.lindex(key, index));
    }

    @Override
    public Long linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        Span span = helper.startNextJedisSpan("linsert", key);
        span.tag("where", where.name());
        span.tag("pivot", Arrays.toString(pivot));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.linsert(key, where, pivot, value));
    }

    @Override
    public Long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        Span span = helper.startNextJedisSpan("linsert", key);
        span.tag("where", where.name());
        span.tag("pivot", pivot);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.linsert(key, where, pivot, value));
    }

    @Override
    public Long llen(byte[] key) {
        Span span = helper.startNextJedisSpan("llen", key);
        return helper.executeInScope(span, () -> delegate.llen(key));
    }

    @Override
    public Long llen(String key) {
        Span span = helper.startNextJedisSpan("llen", key);
        return helper.executeInScope(span, () -> delegate.llen(key));
    }

    @Override
    public byte[] lpop(byte[] key) {
        Span span = helper.startNextJedisSpan("lpop", key);
        return helper.executeInScope(span, () -> delegate.lpop(key));
    }

    @Override
    public String lpop(String key) {
        Span span = helper.startNextJedisSpan("lpop", key);
        return helper.executeInScope(span, () -> delegate.lpop(key));
    }

    @Override
    public Long lpush(byte[] key, byte[]... strings) {
        Span span = helper.startNextJedisSpan("lpush", key);
        span.tag("strings", TagUtils.toString(strings));
        return helper.executeInScope(span, () -> delegate.lpush(key, strings));
    }

    @Override
    public Long lpush(String key, String... strings) {
        Span span = helper.startNextJedisSpan("lpush", key);
        span.tag("strings", Arrays.toString(strings));
        return helper.executeInScope(span, () -> delegate.lpush(key, strings));
    }

    @Override
    public Long lpushx(byte[] key, byte[]... string) {
        Span span = helper.startNextJedisSpan("lpushx", key);
        span.tag("string", TagUtils.toString(string));
        return helper.executeInScope(span, () -> delegate.lpushx(key, string));
    }

    @Override
    public Long lpushx(String key, String... string) {
        Span span = helper.startNextJedisSpan("lpushx", key);
        span.tag("string", Arrays.toString(string));
        return helper.executeInScope(span, () -> delegate.lpushx(key, string));
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("lrange", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.lrange(key, start, end));
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("lrange", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.lrange(key, start, end));
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        Span span = helper.startNextJedisSpan("lrem", key);
        span.tag("count", String.valueOf(count));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.lrem(key, count, value));
    }

    @Override
    public Long lrem(String key, long count, String value) {
        Span span = helper.startNextJedisSpan("lrem", key);
        span.tag("count", String.valueOf(count));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.lrem(key, count, value));
    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {
        Span span = helper.startNextJedisSpan("lset", key);
        span.tag("index", String.valueOf(index));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.lset(key, index, value));
    }

    @Override
    public String lset(String key, long index, String value) {
        Span span = helper.startNextJedisSpan("lset", key);
        span.tag("index", String.valueOf(index));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.lset(key, index, value));
    }

    @Override
    public String ltrim(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("ltrim", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.ltrim(key, start, end));
    }

    @Override
    public String ltrim(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("ltrim", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.ltrim(key, start, end));
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        Span span = helper.startNextJedisSpan("mget");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.mget(keys));
    }

    @Override
    public List<String> mget(String... keys) {
        Span span = helper.startNextJedisSpan("mget", keys);
        return helper.executeInScope(span, () -> delegate.mget(keys));
    }

    @Override
    public String migrate(byte[] host, int port, byte[] key, int destinationDb, int timeout) {
        Span span = helper.startNextJedisSpan("migrate", key);
        span.tag("host", Arrays.toString(host));
        span.tag("destinationDb", String.valueOf(destinationDb));
        span.tag("timeout", String.valueOf(timeout));
        span.tag("port", String.valueOf(port));
        return helper.executeInScope(span, () -> delegate.migrate(host, port, key, destinationDb, timeout));
    }

    @Override
    public String migrate(String host, int port, String key, int destinationDb, int timeout) {
        Span span = helper.startNextJedisSpan("migrate", key);
        span.tag("host", host);
        span.tag("destinationDb", String.valueOf(destinationDb));
        span.tag("timeout", String.valueOf(timeout));
        span.tag("port", String.valueOf(port));
        return helper.executeInScope(span, () -> delegate.migrate(host, port, key, destinationDb, timeout));
    }

    @Override
    public void monitor(JedisMonitor jedisMonitor) {
        Span span = helper.startNextJedisSpan("monitor");
        helper.executeInScope(span, () -> delegate.monitor(jedisMonitor));
    }

    @Override
    public Long move(byte[] key, int dbIndex) {
        Span span = helper.startNextJedisSpan("move", key);
        span.tag("dbIndex", String.valueOf(dbIndex));
        return helper.executeInScope(span, () -> delegate.move(key, dbIndex));
    }

    @Override
    public Long move(String key, int dbIndex) {
        Span span = helper.startNextJedisSpan("move", key);
        span.tag("dbIndex", String.valueOf(dbIndex));
        return helper.executeInScope(span, () -> delegate.move(key, dbIndex));
    }

    @Override
    public String mset(byte[]... keysvalues) {
        Span span = helper.startNextJedisSpan("mset");
        span.tag("keysvalues", TagUtils.toString(keysvalues));
        return helper.executeInScope(span, () -> delegate.mset(keysvalues));
    }

    @Override
    public String mset(String... keysvalues) {
        Span span = helper.startNextJedisSpan("mset");
        span.tag("keysvalues", Arrays.toString(keysvalues));
        return helper.executeInScope(span, () -> delegate.mset(keysvalues));
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        Span span = helper.startNextJedisSpan("msetnx");
        span.tag("keysvalues", TagUtils.toString(keysvalues));
        return helper.executeInScope(span, () -> delegate.msetnx(keysvalues));
    }

    @Override
    public Long msetnx(String... keysvalues) {
        Span span = helper.startNextJedisSpan("msetnx");
        span.tag("keysvalues", Arrays.toString(keysvalues));
        return helper.executeInScope(span, () -> delegate.msetnx(keysvalues));
    }

    @Override
    public Transaction multi() {
        Span span = helper.startNextJedisSpan("multi");
        return helper.executeInScope(span, () -> delegate.multi());
    }

    @Override
    public List<Object> multi(TransactionBlock jedisTransaction) {
        Span span = helper.startNextJedisSpan("multi");
        return helper.executeInScope(span, () -> delegate.multi(jedisTransaction));
    }

    @Override
    public byte[] objectEncoding(byte[] key) {
        Span span = helper.startNextJedisSpan("objectEncoding", key);
        return helper.executeInScope(span, () -> delegate.objectEncoding(key));
    }

    @Override
    public String objectEncoding(String string) {
        Span span = helper.startNextJedisSpan("objectEncoding");
        span.tag("string", string);
        return helper.executeInScope(span, () -> delegate.objectEncoding(string));
    }

    @Override
    public Long objectIdletime(byte[] key) {
        Span span = helper.startNextJedisSpan("objectIdletime", key);
        return helper.executeInScope(span, () -> delegate.objectIdletime(key));
    }

    @Override
    public Long objectIdletime(String string) {
        Span span = helper.startNextJedisSpan("objectIdletime");
        span.tag("string", string);
        return helper.executeInScope(span, () -> delegate.objectIdletime(string));
    }

    @Override
    public Long objectRefcount(byte[] key) {
        Span span = helper.startNextJedisSpan("objectRefcount", key);
        return helper.executeInScope(span, () -> delegate.objectRefcount(key));
    }

    @Override
    public Long objectRefcount(String string) {
        Span span = helper.startNextJedisSpan("objectRefcount");
        span.tag("string", string);
        return helper.executeInScope(span, () -> delegate.objectRefcount(string));
    }

    @Override
    public Long persist(byte[] key) {
        Span span = helper.startNextJedisSpan("persist", key);
        return helper.executeInScope(span, () -> delegate.persist(key));
    }

    @Override
    public Long persist(String key) {
        Span span = helper.startNextJedisSpan("persist", key);
        return helper.executeInScope(span, () -> delegate.persist(key));
    }

    @Override
    public Long pexpire(byte[] key, int milliseconds) {
        Span span = helper.startNextJedisSpan("pexpire", key);
        span.tag("milliseconds", String.valueOf(milliseconds));
        return helper.executeInScope(span, () -> delegate.pexpire(key, milliseconds));
    }

    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        Span span = helper.startNextJedisSpan("pexpire", key);
        span.tag("milliseconds", String.valueOf(milliseconds));
        return helper.executeInScope(span, () -> delegate.pexpire(key, milliseconds));
    }

    @Override
    public Long pexpire(String key, int milliseconds) {
        Span span = helper.startNextJedisSpan("pexpire", key);
        span.tag("milliseconds", String.valueOf(milliseconds));
        return helper.executeInScope(span, () -> delegate.pexpire(key, milliseconds));
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        Span span = helper.startNextJedisSpan("pexpire", key);
        span.tag("milliseconds", String.valueOf(milliseconds));
        return helper.executeInScope(span, () -> delegate.pexpire(key, milliseconds));
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        Span span = helper.startNextJedisSpan("pexpireAt", key);
        span.tag("millisecondsTimestamp", String.valueOf(millisecondsTimestamp));
        return helper.executeInScope(span, () -> delegate.pexpireAt(key, millisecondsTimestamp));
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        Span span = helper.startNextJedisSpan("pexpireAt", key);
        span.tag("millisecondsTimestamp", String.valueOf(millisecondsTimestamp));
        return helper.executeInScope(span, () -> delegate.pexpireAt(key, millisecondsTimestamp));
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        Span span = helper.startNextJedisSpan("pfadd", key);
        span.tag("elements", TagUtils.toString(elements));
        return helper.executeInScope(span, () -> delegate.pfadd(key, elements));
    }

    @Override
    public Long pfadd(String key, String... elements) {
        Span span = helper.startNextJedisSpan("pfadd");
        span.tag("elements", Arrays.toString(elements));
        return helper.executeInScope(span, () -> delegate.pfadd(key, elements));
    }

    @Override
    public long pfcount(byte[] key) {
        Span span = helper.startNextJedisSpan("pfcount", key);
        return helper.executeInScope(span, () -> delegate.pfcount(key));
    }

    @Override
    public Long pfcount(byte[]... keys) {
        Span span = helper.startNextJedisSpan("pfcount");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.pfcount(keys));
    }

    @Override
    public long pfcount(String key) {
        Span span = helper.startNextJedisSpan("pfcount", key);
        return helper.executeInScope(span, () -> delegate.pfcount(key));
    }

    @Override
    public long pfcount(String... keys) {
        Span span = helper.startNextJedisSpan("pfcount", keys);
        return helper.executeInScope(span, () -> delegate.pfcount(keys));
    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        Span span = helper.startNextJedisSpan("pfmerge");
        span.tag("destkey", Arrays.toString(destkey));
        span.tag("sourcekeys", TagUtils.toString(sourcekeys));
        return helper.executeInScope(span, () -> delegate.pfmerge(destkey, sourcekeys));
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        Span span = helper.startNextJedisSpan("pfmerge");
        span.tag("destkey", destkey);
        span.tag("sourcekeys", Arrays.toString(sourcekeys));
        return helper.executeInScope(span, () -> delegate.pfmerge(destkey, sourcekeys));
    }

    @Override
    public String ping() {
        Span span = helper.startNextJedisSpan("ping");
        return helper.executeInScope(span, () -> delegate.ping());
    }

    @Override
    public List<Object> pipelined(PipelineBlock jedisPipeline) {
        Span span = helper.startNextJedisSpan("pipelined");
        return helper.executeInScope(span, () -> delegate.pipelined(jedisPipeline));
    }

    @Override
    public Pipeline pipelined() {
        Span span = helper.startNextJedisSpan("pipelined");
        return helper.executeInScope(span, () -> delegate.pipelined());
    }

    @Override
    public String psetex(byte[] key, int milliseconds, byte[] value) {
        Span span = helper.startNextJedisSpan("psetex", key);
        span.tag("value", Arrays.toString(value));
        span.tag("milliseconds", String.valueOf(milliseconds));
        return helper.executeInScope(span, () -> delegate.psetex(key, milliseconds, value));
    }

    @Override
    public String psetex(String key, int milliseconds, String value) {
        Span span = helper.startNextJedisSpan("psetex", key);
        span.tag("milliseconds", String.valueOf(milliseconds));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.psetex(key, milliseconds, value));
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        Span span = helper.startNextJedisSpan("psubscribe");
        span.tag("patterns", Arrays.toString(patterns));
        helper.executeInScope(span, () -> delegate.psubscribe(jedisPubSub, patterns));
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        Span span = helper.startNextJedisSpan("psubscribe");
        span.tag("patterns", Arrays.toString(patterns));
        helper.executeInScope(span, () -> delegate.psubscribe(jedisPubSub, patterns));
    }

    @Override
    public Long pttl(byte[] key) {
        Span span = helper.startNextJedisSpan("pttl", key);
        return helper.executeInScope(span, () -> delegate.pttl(key));
    }

    @Override
    public Long pttl(String key) {
        Span span = helper.startNextJedisSpan("pttl", key);
        return helper.executeInScope(span, () -> delegate.pttl(key));
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        Span span = helper.startNextJedisSpan("publish");
        span.tag("channel", Arrays.toString(channel));
        span.tag("message", Arrays.toString(message));
        return helper.executeInScope(span, () -> delegate.publish(channel, message));
    }

    @Override
    public Long publish(String channel, String message) {
        Span span = helper.startNextJedisSpan("publish");
        span.tag("channel", channel);
        span.tag("message", message);
        return helper.executeInScope(span, () -> delegate.publish(channel, message));
    }

    @Override
    public List<String> pubsubChannels(String pattern) {
        Span span = helper.startNextJedisSpan("pubsubChannels");
        span.tag("pattern", pattern);
        return helper.executeInScope(span, () -> delegate.pubsubChannels(pattern));
    }

    @Override
    public Long pubsubNumPat() {
        Span span = helper.startNextJedisSpan("pubsubNumPat");
        return helper.executeInScope(span, () -> delegate.pubsubNumPat());
    }

    @Override
    public Map<String, String> pubsubNumSub(String... channels) {
        Span span = helper.startNextJedisSpan("pubsubNumSub");
        span.tag("channels", Arrays.toString(channels));
        return helper.executeInScope(span, () -> delegate.pubsubNumSub(channels));
    }

    @Override
    public String quit() {
        Span span = helper.startNextJedisSpan("quit");
        return helper.executeInScope(span, () -> delegate.quit());
    }

    @Override
    public byte[] randomBinaryKey() {
        Span span = helper.startNextJedisSpan("randomBinaryKey");
        return helper.executeInScope(span, () -> delegate.randomBinaryKey());
    }

    @Override
    public String randomKey() {
        Span span = helper.startNextJedisSpan("randomKey");
        return helper.executeInScope(span, () -> delegate.randomKey());
    }


    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        Span span = helper.startNextJedisSpan("rename");
        span.tag("oldkey", Arrays.toString(oldkey));
        span.tag("newkey", Arrays.toString(newkey));
        return helper.executeInScope(span, () -> delegate.rename(oldkey, newkey));
    }

    @Override
    public String rename(String oldkey, String newkey) {
        Span span = helper.startNextJedisSpan("rename");
        span.tag("oldKey", Objects.toString(oldkey));
        span.tag("newKey", Objects.toString(newkey));
        return helper.executeInScope(span, () -> delegate.rename(oldkey, newkey));
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        Span span = helper.startNextJedisSpan("renamenx");
        span.tag("oldkey", Arrays.toString(oldkey));
        span.tag("newkey", Arrays.toString(newkey));
        return helper.executeInScope(span, () -> delegate.renamenx(oldkey, newkey));
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        Span span = helper.startNextJedisSpan("renamenx");
        span.tag("oldKey", Objects.toString(oldkey));
        span.tag("newKey", Objects.toString(newkey));
        return helper.executeInScope(span, () -> delegate.renamenx(oldkey, newkey));
    }

    @Override
    public void resetState() {
        Span span = helper.startNextJedisSpan("resetState");
        helper.executeInScope(span, () -> delegate.resetState());
    }

    @Override
    public String restore(byte[] key, int ttl, byte[] serializedValue) {
        Span span = helper.startNextJedisSpan("restore", key);
        span.tag("ttl", String.valueOf(ttl));
        span.tag("serializedValue", Arrays.toString(serializedValue));
        return helper.executeInScope(span, () -> delegate.restore(key, ttl, serializedValue));
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        Span span = helper.startNextJedisSpan("restore", key);
        span.tag("ttl", String.valueOf(ttl));
        span.tag("serializedValue", Arrays.toString(serializedValue));
        return helper.executeInScope(span, () -> delegate.restore(key, ttl, serializedValue));
    }

    @Override
    public byte[] rpop(byte[] key) {
        Span span = helper.startNextJedisSpan("rpop", key);
        return helper.executeInScope(span, () -> delegate.rpop(key));
    }

    @Override
    public String rpop(String key) {
        Span span = helper.startNextJedisSpan("rpop", key);
        return helper.executeInScope(span, () -> delegate.rpop(key));
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        Span span = helper.startNextJedisSpan("rpoplpush");
        span.tag("srckey", Arrays.toString(srckey));
        span.tag("dstkey", Arrays.toString(dstkey));
        return helper.executeInScope(span, () -> delegate.rpoplpush(srckey, dstkey));
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        Span span = helper.startNextJedisSpan("rpoplpush");
        span.tag("srckey", srckey);
        span.tag("dstkey", dstkey);
        return helper.executeInScope(span, () -> delegate.rpoplpush(srckey, dstkey));
    }

    @Override
    public Long rpush(byte[] key, byte[]... strings) {
        Span span = helper.startNextJedisSpan("rpush", key);
        span.tag("strings", TagUtils.toString(strings));
        return helper.executeInScope(span, () -> delegate.rpush(key, strings));
    }

    @Override
    public Long rpush(String key, String... strings) {
        Span span = helper.startNextJedisSpan("rpush", key);
        span.tag("strings", Arrays.toString(strings));
        return helper.executeInScope(span, () -> delegate.rpush(key, strings));
    }

    @Override
    public Long rpushx(byte[] key, byte[]... string) {
        Span span = helper.startNextJedisSpan("rpushx", key);
        span.tag("string", TagUtils.toString(string));
        return helper.executeInScope(span, () -> delegate.rpushx(key, string));
    }

    @Override
    public Long rpushx(String key, String... string) {
        Span span = helper.startNextJedisSpan("rpushx", key);
        span.tag("string", Arrays.toString(string));
        return helper.executeInScope(span, () -> delegate.rpushx(key, string));
    }

    @Override
    public Long sadd(byte[] key, byte[]... members) {
        Span span = helper.startNextJedisSpan("sadd", key);
        span.tag("members", Arrays.toString(members));
        return helper.executeInScope(span, () -> delegate.sadd(key, members));
    }

    @Override
    public Long sadd(String key, String... members) {
        Span span = helper.startNextJedisSpan("sadd", key);
        span.tag("members", Arrays.toString(members));
        return helper.executeInScope(span, () -> delegate.sadd(key, members));
    }

    @Override
    public String save() {
        Span span = helper.startNextJedisSpan("save");
        return helper.executeInScope(span, () -> delegate.save());
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor) {
        Span span = helper.startNextJedisSpan("scan");
        span.tag("cursor", Arrays.toString(cursor));
        return helper.executeInScope(span, () -> delegate.scan(cursor));
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("scan");
        span.tag("cursor", Arrays.toString(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.scan(cursor, params));
    }

    @Override
    public ScanResult<String> scan(int cursor) {
        Span span = helper.startNextJedisSpan("scan");
        span.tag("cursor", String.valueOf(cursor));
        return helper.executeInScope(span, () -> delegate.scan(cursor));
    }

    @Override
    public ScanResult<String> scan(int cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("scan");
        span.tag("cursor", String.valueOf(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.scan(cursor, params));
    }

    @Override
    public ScanResult<String> scan(String cursor) {
        Span span = helper.startNextJedisSpan("scan");
        span.tag("cursor", cursor);
        return helper.executeInScope(span, () -> delegate.scan(cursor));
    }

    @Override
    public ScanResult<String> scan(String cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("scan");
        span.tag("cursor", cursor);
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.scan(cursor, params));
    }

    @Override
    public Long scard(byte[] key) {
        Span span = helper.startNextJedisSpan("scard", key);
        return helper.executeInScope(span, () -> delegate.scard(key));
    }

    @Override
    public Long scard(String key) {
        Span span = helper.startNextJedisSpan("scard", key);
        return helper.executeInScope(span, () -> delegate.scard(key));
    }


    @Override
    public List<Long> scriptExists(byte[]... sha1) {
        Span span = helper.startNextJedisSpan("scriptExists");
        span.tag("sha1", TagUtils.toString(sha1));
        return helper.executeInScope(span, () -> delegate.scriptExists(sha1));
    }

    @Override
    public Boolean scriptExists(String sha1) {
        Span span = helper.startNextJedisSpan("scriptExists");
        span.tag("sha1", sha1);
        return helper.executeInScope(span, () -> delegate.scriptExists(sha1));
    }

    @Override
    public List<Boolean> scriptExists(String... sha1) {
        Span span = helper.startNextJedisSpan("scriptExists");
        span.tag("sha1", Arrays.toString(sha1));
        return helper.executeInScope(span, () -> delegate.scriptExists(sha1));
    }

    @Override
    public String scriptFlush() {
        Span span = helper.startNextJedisSpan("scriptFlush");
        return helper.executeInScope(span, () -> delegate.scriptFlush());
    }

    @Override
    public String scriptKill() {
        Span span = helper.startNextJedisSpan("scriptKill");
        return helper.executeInScope(span, () -> delegate.scriptKill());
    }

    @Override
    public byte[] scriptLoad(byte[] script) {
        Span span = helper.startNextJedisSpan("scriptLoad");
        span.tag("script", Arrays.toString(script));
        return helper.executeInScope(span, () -> delegate.scriptLoad(script));
    }

    @Override
    public String scriptLoad(String script) {
        Span span = helper.startNextJedisSpan("scriptLoad");
        span.tag("script", script);
        return helper.executeInScope(span, () -> delegate.scriptLoad(script));
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        Span span = helper.startNextJedisSpan("sdiff");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.sdiff(keys));
    }

    @Override
    public Set<String> sdiff(String... keys) {
        Span span = helper.startNextJedisSpan("sdiff", keys);
        return helper.executeInScope(span, () -> delegate.sdiff(keys));
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        Span span = helper.startNextJedisSpan("sdiffstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.sdiffstore(dstkey, keys));
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        Span span = helper.startNextJedisSpan("sdiffstore", keys);
        span.tag("dstkey", dstkey);
        return helper.executeInScope(span, () -> delegate.sdiffstore(dstkey, keys));
    }

    @Override
    public String select(int index) {
        Span span = helper.startNextJedisSpan("select");
        span.tag("index", String.valueOf(index));
        return helper.executeInScope(span, () -> delegate.select(index));
    }

    @Override
    public String sentinelFailover(String masterName) {
        Span span = helper.startNextJedisSpan("sentinelFailover");
        return helper.executeInScope(span, () -> delegate.sentinelFailover(masterName));
    }

    @Override
    public List<String> sentinelGetMasterAddrByName(String masterName) {
        Span span = helper.startNextJedisSpan("sentinelGetMasterAddrByName");
        span.tag("masterName", masterName);
        return helper.executeInScope(span, () -> delegate.sentinelGetMasterAddrByName(masterName));
    }

    @Override
    public List<Map<String, String>> sentinelMasters() {
        Span span = helper.startNextJedisSpan("sentinelMasters");
        return helper.executeInScope(span, () -> delegate.sentinelMasters());
    }

    @Override
    public String sentinelMonitor(String masterName, String ip, int port, int quorum) {
        Span span = helper.startNextJedisSpan("sentinelMonitor");
        span.tag("masterName", masterName);
        span.tag("ip", ip);
        span.tag("port", String.valueOf(port));
        span.tag("quorum", String.valueOf(quorum));
        return helper.executeInScope(span, () -> delegate.sentinelMonitor(masterName, ip, port, quorum));
    }

    @Override
    public String sentinelRemove(String masterName) {
        Span span = helper.startNextJedisSpan("sentinelRemove");
        span.tag("masterName", masterName);
        return helper.executeInScope(span, () -> delegate.sentinelRemove(masterName));
    }

    @Override
    public Long sentinelReset(String pattern) {
        Span span = helper.startNextJedisSpan("sentinelReset");
        span.tag("pattern", pattern);
        return helper.executeInScope(span, () -> delegate.sentinelReset(pattern));
    }

    @Override
    public String sentinelSet(String masterName, Map<String, String> parameterMap) {
        Span span = helper.startNextJedisSpan("sentinelSet");
        span.tag("masterName", masterName);
        span.tag("parameterMap", TagUtils.toString(parameterMap));
        return helper.executeInScope(span, () -> delegate.sentinelSet(masterName, parameterMap));
    }

    @Override
    public List<Map<String, String>> sentinelSlaves(String masterName) {
        Span span = helper.startNextJedisSpan("sentinelSlaves");
        span.tag("masterName", masterName);
        return helper.executeInScope(span, () -> delegate.sentinelSlaves(masterName));
    }

    @Override
    public String set(byte[] key, byte[] value) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.set(key, value));
    }

    @Override
    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("nxxx", Arrays.toString(nxxx));
        span.tag("expx", Arrays.toString(expx));
        span.tag("time", String.valueOf(time));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.set(key, value, nxxx, expx, time));
    }

    @Override
    public String set(byte[] key, byte[] value, byte[] nxxx) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("nxxx", Arrays.toString(nxxx));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.set(key, value, nxxx));
    }

    @Override
    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("nxxx", Arrays.toString(nxxx));
        span.tag("expx", Arrays.toString(expx));
        span.tag("time", String.valueOf(time));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.set(key, value, nxxx, expx, time));
    }

    @Override
    public String set(String key, String value) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.set(key, value));
    }

    @Override
    public String set(String key, String value, String nxxx, String expx, long time) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("nxxx", nxxx);
        span.tag("expx", expx);
        span.tag("time", String.valueOf(time));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.set(key, value, nxxx, expx, time));
    }

    @Override
    public String set(String key, String value, String nxxx) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("value", value);
        span.tag("nxxx", nxxx);
        return helper.executeInScope(span, () -> delegate.set(key, value, nxxx));
    }

    @Override
    public String set(String key, String value, String nxxx, String expx, int time) {
        Span span = helper.startNextJedisSpan("set", key);
        span.tag("value", value);
        span.tag("nxxx", nxxx);
        span.tag("expx", expx);
        span.tag("time", String.valueOf(time));
        return helper.executeInScope(span, () -> delegate.set(key, value, nxxx, expx, time));
    }

    @Override
    public void setDataSource(Pool<Jedis> jedisPool) {
        Span span = helper.startNextJedisSpan("setDataSource");
        helper.executeInScope(span, () -> delegate.setDataSource(jedisPool));
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        Span span = helper.startNextJedisSpan("setbit", key);
        span.tag("offset", String.valueOf(offset));
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.setbit(key, offset, value));
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        Span span = helper.startNextJedisSpan("setbit", key);
        span.tag("offset", String.valueOf(offset));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.setbit(key, offset, value));
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        Span span = helper.startNextJedisSpan("setbit", key);
        span.tag("offset", String.valueOf(offset));
        span.tag("value", String.valueOf(value));
        return helper.executeInScope(span, () -> delegate.setbit(key, offset, value));
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        Span span = helper.startNextJedisSpan("setbit", key);
        span.tag("offset", String.valueOf(offset));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.setbit(key, offset, value));
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        Span span = helper.startNextJedisSpan("setex", key);
        span.tag("value", Arrays.toString(value));
        span.tag("seconds", String.valueOf(seconds));
        return helper.executeInScope(span, () -> delegate.setex(key, seconds, value));
    }

    @Override
    public String setex(String key, int seconds, String value) {
        Span span = helper.startNextJedisSpan("setex", key);
        span.tag("seconds", String.valueOf(seconds));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.setex(key, seconds, value));
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        Span span = helper.startNextJedisSpan("setnx", key);
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.setnx(key, value));
    }

    @Override
    public Long setnx(String key, String value) {
        Span span = helper.startNextJedisSpan("setnx", key);
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.setnx(key, value));
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        Span span = helper.startNextJedisSpan("setrange", key);
        span.tag("offset", String.valueOf(offset));
        span.tag("value", Arrays.toString(value));
        return helper.executeInScope(span, () -> delegate.setrange(key, offset, value));
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        Span span = helper.startNextJedisSpan("setrange", key);
        span.tag("offset", String.valueOf(offset));
        span.tag("value", value);
        return helper.executeInScope(span, () -> delegate.setrange(key, offset, value));
    }

    @Override
    public String shutdown() {
        Span span = helper.startNextJedisSpan("shutdown");
        return helper.executeInScope(span, () -> delegate.shutdown());
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        Span span = helper.startNextJedisSpan("sinter");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.sinter(keys));
    }

    @Override
    public Set<String> sinter(String... keys) {
        Span span = helper.startNextJedisSpan("sinter", keys);
        return helper.executeInScope(span, () -> delegate.sinter(keys));
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        Span span = helper.startNextJedisSpan("sinterstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.sinterstore(dstkey, keys));
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        Span span = helper.startNextJedisSpan("sinterstore", keys);
        span.tag("dstkey", dstkey);
        return helper.executeInScope(span, () -> delegate.sinterstore(dstkey, keys));
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        Span span = helper.startNextJedisSpan("sismember", key);
        span.tag("member", Arrays.toString(member));
        return helper.executeInScope(span, () -> delegate.sismember(key, member));
    }

    @Override
    public Boolean sismember(String key, String member) {
        Span span = helper.startNextJedisSpan("sismember", key);
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.sismember(key, member));
    }

    @Override
    public String slaveof(String host, int port) {
        Span span = helper.startNextJedisSpan("slaveof");
        span.tag("host", host);
        span.tag("port", String.valueOf(port));
        return helper.executeInScope(span, () -> delegate.slaveof(host, port));
    }

    @Override
    public String slaveofNoOne() {
        Span span = helper.startNextJedisSpan("slaveofNoOne");
        return helper.executeInScope(span, () -> delegate.slaveofNoOne());
    }

    @Override
    public List<Slowlog> slowlogGet() {
        Span span = helper.startNextJedisSpan("slowlogGet");
        return helper.executeInScope(span, () -> delegate.slowlogGet());
    }

    @Override
    public List<Slowlog> slowlogGet(long entries) {
        Span span = helper.startNextJedisSpan("slowlogGet");
        span.tag("entries", String.valueOf(entries));
        return helper.executeInScope(span, () -> delegate.slowlogGet(entries));
    }

    @Override
    public List<byte[]> slowlogGetBinary() {
        Span span = helper.startNextJedisSpan("slowlogGetBinary");
        return helper.executeInScope(span, () -> delegate.slowlogGetBinary());
    }

    @Override
    public List<byte[]> slowlogGetBinary(long entries) {
        Span span = helper.startNextJedisSpan("slowlogGetBinary");
        span.tag("entries", String.valueOf(entries));
        return helper.executeInScope(span, () -> delegate.slowlogGetBinary(entries));
    }

    @Override
    public Long slowlogLen() {
        Span span = helper.startNextJedisSpan("slowlogLen");
        return helper.executeInScope(span, () -> delegate.slowlogLen());
    }

    @Override
    public String slowlogReset() {
        Span span = helper.startNextJedisSpan("slowlogReset");
        return helper.executeInScope(span, () -> delegate.slowlogReset());
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        Span span = helper.startNextJedisSpan("smembers", key);
        return helper.executeInScope(span, () -> delegate.smembers(key));
    }

    @Override
    public Set<String> smembers(String key) {
        Span span = helper.startNextJedisSpan("smembers", key);
        return helper.executeInScope(span, () -> delegate.smembers(key));
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        Span span = helper.startNextJedisSpan("smove");
        span.tag("srckey", Arrays.toString(srckey));
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("member", Arrays.toString(member));
        return helper.executeInScope(span, () -> delegate.smove(srckey, dstkey, member));
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        Span span = helper.startNextJedisSpan("smove");
        span.tag("srckey", srckey);
        span.tag("dstkey", dstkey);
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.smove(srckey, dstkey, member));
    }

    @Override
    public List<byte[]> sort(byte[] key) {
        Span span = helper.startNextJedisSpan("sort", key);
        return helper.executeInScope(span, () -> delegate.sort(key));
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        Span span = helper.startNextJedisSpan("sort", key);
        span.tag("sortingParameters", TagUtils.toString(sortingParameters.getParams()));
        return helper.executeInScope(span, () -> delegate.sort(key, sortingParameters));
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        Span span = helper.startNextJedisSpan("sort", key);
        span.tag("sortingParameters", TagUtils.toString(sortingParameters.getParams()));
        span.tag("dstkey", Arrays.toString(dstkey));
        return helper.executeInScope(span, () -> delegate.sort(key, sortingParameters, dstkey));
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        Span span = helper.startNextJedisSpan("sort", key);
        span.tag("dstkey", Arrays.toString(dstkey));
        return helper.executeInScope(span, () -> delegate.sort(key, dstkey));
    }

    @Override
    public List<String> sort(String key) {
        Span span = helper.startNextJedisSpan("sort", key);
        return helper.executeInScope(span, () -> delegate.sort(key));
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        Span span = helper.startNextJedisSpan("sort", key);
        span.tag("sortingParameters", TagUtils.toString(sortingParameters.getParams()));
        return helper.executeInScope(span, () -> delegate.sort(key, sortingParameters));
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        Span span = helper.startNextJedisSpan("sort", key);
        span.tag("sortingParameters", TagUtils.toString(sortingParameters.getParams()));
        span.tag("dstkey", dstkey);
        return helper.executeInScope(span, () -> delegate.sort(key, sortingParameters, dstkey));
    }

    @Override
    public Long sort(String key, String dstkey) {
        Span span = helper.startNextJedisSpan("sort", key);
        span.tag("dstkey", dstkey);
        return helper.executeInScope(span, () -> delegate.sort(key, dstkey));
    }

    @Override
    public byte[] spop(byte[] key) {
        Span span = helper.startNextJedisSpan("spop", key);
        return helper.executeInScope(span, () -> delegate.spop(key));
    }


    @Override
    public String spop(String key) {
        Span span = helper.startNextJedisSpan("spop", key);
        return helper.executeInScope(span, () -> delegate.spop(key));
    }

    @Override
    public byte[] srandmember(byte[] key) {
        Span span = helper.startNextJedisSpan("srandmember", key);
        return helper.executeInScope(span, () -> delegate.srandmember(key));
    }

    @Override
    public List<byte[]> srandmember(byte[] key, int count) {
        Span span = helper.startNextJedisSpan("srandmember", key);
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.srandmember(key, count));
    }

    @Override
    public String srandmember(String key) {
        Span span = helper.startNextJedisSpan("srandmember", key);
        return helper.executeInScope(span, () -> delegate.srandmember(key));
    }

    @Override
    public List<String> srandmember(String key, int count) {
        Span span = helper.startNextJedisSpan("srandmember", key);
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.srandmember(key, count));
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        Span span = helper.startNextJedisSpan("srem", key);
        span.tag("member", Arrays.toString(member));
        return helper.executeInScope(span, () -> delegate.srem(key, member));
    }

    @Override
    public Long srem(String key, String... members) {
        Span span = helper.startNextJedisSpan("srem", key);
        span.tag("members", Arrays.toString(members));
        return helper.executeInScope(span, () -> delegate.srem(key, members));
    }

    @Override
    public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
        Span span = helper.startNextJedisSpan("sscan", key);
        span.tag("cursor", Arrays.toString(cursor));
        return helper.executeInScope(span, () -> delegate.sscan(key, cursor));
    }

    @Override
    public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("sscan", key);
        span.tag("cursor", Arrays.toString(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.sscan(key, cursor, params));
    }

    @Override
    public ScanResult<String> sscan(String key, int cursor) {
        Span span = helper.startNextJedisSpan("sscan", key);
        span.tag("cursor", String.valueOf(cursor));
        return helper.executeInScope(span, () -> delegate.sscan(key, cursor));
    }

    @Override
    public ScanResult<String> sscan(String key, int cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("sscan", key);
        span.tag("cursor", String.valueOf(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.sscan(key, cursor, params));
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        Span span = helper.startNextJedisSpan("sscan", key);
        span.tag("cursor", cursor);
        return helper.executeInScope(span, () -> delegate.sscan(key, cursor));
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("sscan", key);
        span.tag("cursor", cursor);
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.sscan(key, cursor, params));
    }

    @Override
    public Long strlen(byte[] key) {
        Span span = helper.startNextJedisSpan("strlen", key);
        return helper.executeInScope(span, () -> delegate.strlen(key));
    }

    @Override
    public Long strlen(String key) {
        Span span = helper.startNextJedisSpan("strlen", key);
        return helper.executeInScope(span, () -> delegate.strlen(key));
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        Span span = helper.startNextJedisSpan("subscribe");
        span.tag("channels", Arrays.toString(channels));
        helper.executeInScope(span, () -> delegate.subscribe(jedisPubSub, channels));
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        Span span = helper.startNextJedisSpan("subscribe");
        span.tag("channels", Arrays.toString(channels));
        helper.executeInScope(span, () -> delegate.subscribe(jedisPubSub, channels));
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        Span span = helper.startNextJedisSpan("substr", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.substr(key, start, end));
    }

    @Override
    public String substr(String key, int start, int end) {
        Span span = helper.startNextJedisSpan("substr", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.substr(key, start, end));
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        Span span = helper.startNextJedisSpan("sunion");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.sunion(keys));
    }

    @Override
    public Set<String> sunion(String... keys) {
        Span span = helper.startNextJedisSpan("sunion", keys);
        return helper.executeInScope(span, () -> delegate.sunion(keys));
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        Span span = helper.startNextJedisSpan("sunionstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.sunionstore(dstkey, keys));
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        Span span = helper.startNextJedisSpan("sunionstore", keys);
        span.tag("dstkey", dstkey);
        return helper.executeInScope(span, () -> delegate.sunionstore(dstkey, keys));
    }

    @Override
    public void sync() {
        Span span = helper.startNextJedisSpan("sync");
        helper.executeInScope(span, () -> delegate.sync());
    }

    @Override
    public List<String> time() {
        Span span = helper.startNextJedisSpan("time");
        return helper.executeInScope(span, () -> delegate.time());
    }

    @Override
    public Long ttl(byte[] key) {
        Span span = helper.startNextJedisSpan("ttl", key);
        return helper.executeInScope(span, () -> delegate.ttl(key));
    }

    @Override
    public Long ttl(String key) {
        Span span = helper.startNextJedisSpan("ttl", key);
        return helper.executeInScope(span, () -> delegate.ttl(key));
    }

    @Override
    public String type(byte[] key) {
        Span span = helper.startNextJedisSpan("type", key);
        return helper.executeInScope(span, () -> delegate.type(key));
    }

    @Override
    public String type(String key) {
        Span span = helper.startNextJedisSpan("type", key);
        return helper.executeInScope(span, () -> delegate.type(key));
    }

    @Override
    public String unwatch() {
        Span span = helper.startNextJedisSpan("unwatch");
        return helper.executeInScope(span, () -> delegate.unwatch());
    }

    @Override
    public Long waitReplicas(int replicas, long timeout) {
        Span span = helper.startNextJedisSpan("waitReplicas");
        span.tag("replicas", String.valueOf(replicas));
        span.tag("timeout", String.valueOf(timeout));
        return helper.executeInScope(span, () -> delegate.waitReplicas(replicas, timeout));
    }

    @Override
    public String watch(byte[]... keys) {
        Span span = helper.startNextJedisSpan("watch");
        span.tag("keys", TagUtils.toString(keys));
        return helper.executeInScope(span, () -> delegate.watch(keys));
    }

    @Override
    public String watch(String... keys) {
        Span span = helper.startNextJedisSpan("watch", keys);
        return helper.executeInScope(span, () -> delegate.watch(keys));
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        Span span = helper.startNextJedisSpan("zadd", key);
        span.tag("member", Arrays.toString(member));
        span.tag("score", String.valueOf(score));
        return helper.executeInScope(span, () -> delegate.zadd(key, score, member));
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        Span span = helper.startNextJedisSpan("zadd", key);
        span.tag("scoreMembers", TagUtils.toStringMap2(scoreMembers));
        return helper.executeInScope(span, () -> delegate.zadd(key, scoreMembers));
    }

    @Override
    public Long zadd(String key, double score, String member) {
        Span span = helper.startNextJedisSpan("zadd", key);
        span.tag("score", String.valueOf(score));
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.zadd(key, score, member));
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        Span span = helper.startNextJedisSpan("zadd", key);
        span.tag("scoreMembers", TagUtils.toString(scoreMembers));
        return helper.executeInScope(span, () -> delegate.zadd(key, scoreMembers));
    }

    @Override
    public Long zcard(byte[] key) {
        Span span = helper.startNextJedisSpan("zcard", key);
        return helper.executeInScope(span, () -> delegate.zcard(key));
    }

    @Override
    public Long zcard(String key) {
        Span span = helper.startNextJedisSpan("zcard", key);
        return helper.executeInScope(span, () -> delegate.zcard(key));
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        Span span = helper.startNextJedisSpan("zcount", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zcount(key, min, max));
    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {
        Span span = helper.startNextJedisSpan("zcount", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        return helper.executeInScope(span, () -> delegate.zcount(key, min, max));
    }

    @Override
    public Long zcount(String key, double min, double max) {
        Span span = helper.startNextJedisSpan("zcount", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zcount(key, min, max));
    }

    @Override
    public Long zcount(String key, String min, String max) {
        Span span = helper.startNextJedisSpan("zcount", key);
        span.tag("min", min);
        span.tag("max", max);
        return helper.executeInScope(span, () -> delegate.zcount(key, min, max));
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        Span span = helper.startNextJedisSpan("zincrby", key);
        span.tag("member", Arrays.toString(member));
        span.tag("score", String.valueOf(score));
        return helper.executeInScope(span, () -> delegate.zincrby(key, score, member));
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        Span span = helper.startNextJedisSpan("zincrby", key);
        span.tag("score", String.valueOf(score));
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.zincrby(key, score, member));
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        Span span = helper.startNextJedisSpan("zinterstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("sets", TagUtils.toString(sets));
        return helper.executeInScope(span, () -> delegate.zinterstore(dstkey, sets));
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        Span span = helper.startNextJedisSpan("zinterstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("params", TagUtils.toString(params.getParams()));
        span.tag("sets", TagUtils.toString(sets));
        return helper.executeInScope(span, () -> delegate.zinterstore(dstkey, params, sets));
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        Span span = helper.startNextJedisSpan("zinterstore");
        span.tag("dstkey", dstkey);
        span.tag("sets", Arrays.toString(sets));
        return helper.executeInScope(span, () -> delegate.zinterstore(dstkey, sets));
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        Span span = helper.startNextJedisSpan("zinterstore");
        span.tag("dstkey", dstkey);
        span.tag("params", TagUtils.toString(params.getParams()));
        span.tag("sets", Arrays.toString(sets));
        return helper.executeInScope(span, () -> delegate.zinterstore(dstkey, params, sets));
    }

    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        Span span = helper.startNextJedisSpan("zlexcount");
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        return helper.executeInScope(span, () -> delegate.zlexcount(key, min, max));
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        Span span = helper.startNextJedisSpan("zlexcount", key);
        span.tag("min", min);
        span.tag("max", max);
        return helper.executeInScope(span, () -> delegate.zlexcount(key, min, max));
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrange", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrange(key, start, end));
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrange", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrange(key, start, end));
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        Span span = helper.startNextJedisSpan("zrangeByLex", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        return helper.executeInScope(span, () -> delegate.zrangeByLex(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrangeByLex", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrangeByLex(key, min, max, offset, count));
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        Span span = helper.startNextJedisSpan("zrangeByLex", key);
        span.tag("min", min);
        span.tag("max", max);
        return helper.executeInScope(span, () -> delegate.zrangeByLex(key, min, max));
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrangeByLex", key);
        span.tag("min", min);
        span.tag("max", max);
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrangeByLex(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", min);
        span.tag("max", max);
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrangeByScore", key);
        span.tag("min", min);
        span.tag("max", max);
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        return helper.executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset,
                                              int count) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset,
                                              int count) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", min);
        span.tag("max", max);
        return helper.executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset,
                                              int count) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset,
                                              int count) {
        Span span = helper.startNextJedisSpan("zrangeByScoreWithScores", key);
        span.tag("min", min);
        span.tag("max", max);
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrangeWithScores", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrangeWithScores(key, start, end));
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrangeWithScores", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrangeWithScores(key, start, end));
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        Span span = helper.startNextJedisSpan("zrank", key);
        span.tag("member", Arrays.toString(member));
        return helper.executeInScope(span, () -> delegate.zrank(key, member));
    }

    @Override
    public Long zrank(String key, String member) {
        Span span = helper.startNextJedisSpan("zrank", key);
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.zrank(key, member));
    }

    @Override
    public Long zrem(byte[] key, byte[]... members) {
        Span span = helper.startNextJedisSpan("zrem", key);
        span.tag("members", Arrays.toString(members));
        return helper.executeInScope(span, () -> delegate.zrem(key, members));
    }

    @Override
    public Long zrem(String key, String... members) {
        Span span = helper.startNextJedisSpan("zrem", key);
        span.tag("members", Arrays.toString(members));
        return helper.executeInScope(span, () -> delegate.zrem(key, members));
    }

    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        Span span = helper.startNextJedisSpan("zremrangeByLex", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        return helper.executeInScope(span, () -> delegate.zremrangeByLex(key, min, max));
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        Span span = helper.startNextJedisSpan("zremrangeByLex", key);
        span.tag("min", min);
        span.tag("max", max);
        return helper.executeInScope(span, () -> delegate.zremrangeByLex(key, min, max));
    }

    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("zremrangeByRank", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zremrangeByRank(key, start, end));
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("zremrangeByRank", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zremrangeByRank(key, start, end));
    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        Span span = helper.startNextJedisSpan("zremrangeByScore", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zremrangeByScore(key, start, end));
    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        Span span = helper.startNextJedisSpan("zremrangeByScore", key);
        span.tag("start", Arrays.toString(start));
        span.tag("end", Arrays.toString(end));
        return helper.executeInScope(span, () -> delegate.zremrangeByScore(key, start, end));
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        Span span = helper.startNextJedisSpan("zremrangeByScore", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zremrangeByScore(key, start, end));
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        Span span = helper.startNextJedisSpan("zremrangeByScore", key);
        span.tag("start", start);
        span.tag("end", end);
        return helper.executeInScope(span, () -> delegate.zremrangeByScore(key, start, end));
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrevrange", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrevrange(key, start, end));
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrevrange", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrevrange(key, start, end));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("max", Arrays.toString(max));
        span.tag("min", Arrays.toString(min));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("max", String.valueOf(max));
        span.tag("min", String.valueOf(min));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("max", max);
        span.tag("min", min);
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScore", key);
        span.tag("min", min);
        span.tag("max", max);
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset,
                                                 int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("max", Arrays.toString(max));
        span.tag("min", Arrays.toString(min));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset,
                                                 int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("min", Arrays.toString(min));
        span.tag("max", Arrays.toString(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("max", String.valueOf(max));
        span.tag("min", String.valueOf(min));
        return helper.executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset,
                                                 int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("min", String.valueOf(min));
        span.tag("max", String.valueOf(max));
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset,
                                                 int count) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("min", min);
        span.tag("max", max);
        span.tag("offset", String.valueOf(offset));
        span.tag("count", String.valueOf(count));
        return helper
            .executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        Span span = helper.startNextJedisSpan("zrevrangeByScoreWithScores", key);
        span.tag("max", max);
        span.tag("min", min);
        return helper.executeInScope(span, () -> delegate.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrevrangeWithScores", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrevrangeWithScores(key, start, end));
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        Span span = helper.startNextJedisSpan("zrevrangeWithScores", key);
        span.tag("start", String.valueOf(start));
        span.tag("end", String.valueOf(end));
        return helper.executeInScope(span, () -> delegate.zrevrangeWithScores(key, start, end));
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        Span span = helper.startNextJedisSpan("zrevrank", key);
        span.tag("member", Arrays.toString(member));
        return helper.executeInScope(span, () -> delegate.zrevrank(key, member));
    }

    @Override
    public Long zrevrank(String key, String member) {
        Span span = helper.startNextJedisSpan("zrevrank", key);
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.zrevrank(key, member));
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
        Span span = helper.startNextJedisSpan("zscan", key);
        span.tag("cursor", Arrays.toString(cursor));
        return helper.executeInScope(span, () -> delegate.zscan(key, cursor));
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("zscan", key);
        span.tag("cursor", Arrays.toString(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.zscan(key, cursor, params));
    }

    @Override
    public ScanResult<Tuple> zscan(String key, int cursor) {
        Span span = helper.startNextJedisSpan("zscan", key);
        span.tag("cursor", String.valueOf(cursor));
        return helper.executeInScope(span, () -> delegate.zscan(key, cursor));
    }

    @Override
    public ScanResult<Tuple> zscan(String key, int cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("zscan", key);
        span.tag("cursor", String.valueOf(cursor));
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.zscan(key, cursor, params));
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        Span span = helper.startNextJedisSpan("zscan", key);
        span.tag("cursor", cursor);
        return helper.executeInScope(span, () -> delegate.zscan(key, cursor));
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
        Span span = helper.startNextJedisSpan("zscan", key);
        span.tag("cursor", cursor);
        span.tag("params", TagUtils.toString(params.getParams()));
        return helper.executeInScope(span, () -> delegate.zscan(key, cursor, params));
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        Span span = helper.startNextJedisSpan("zscore", key);
        span.tag("member", Arrays.toString(member));
        return helper.executeInScope(span, () -> delegate.zscore(key, member));
    }

    @Override
    public Double zscore(String key, String member) {
        Span span = helper.startNextJedisSpan("zscore", key);
        span.tag("member", member);
        return helper.executeInScope(span, () -> delegate.zscore(key, member));
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        Span span = helper.startNextJedisSpan("zunionstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("sets", TagUtils.toString(sets));
        return helper.executeInScope(span, () -> delegate.zunionstore(dstkey, sets));
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        Span span = helper.startNextJedisSpan("zunionstore");
        span.tag("dstkey", Arrays.toString(dstkey));
        span.tag("params", TagUtils.toString(params.getParams()));
        span.tag("sets", TagUtils.toString(sets));
        return helper.executeInScope(span, () -> delegate.zunionstore(dstkey, params, sets));
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        Span span = helper.startNextJedisSpan("zunionstore");
        span.tag("dstkey", dstkey);
        span.tag("sets", Arrays.toString(sets));
        return helper.executeInScope(span, () -> delegate.zunionstore(dstkey, sets));
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        Span span = helper.startNextJedisSpan("zunionstore");
        span.tag("params", TagUtils.toString(params.getParams()));
        span.tag("sets", Arrays.toString(sets));
        return helper.executeInScope(span, () -> delegate.zunionstore(dstkey, params, sets));
    }


}
