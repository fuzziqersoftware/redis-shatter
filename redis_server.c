#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "debug.h"
#include "resource.h"
#include "redis_socket.h"
#include "redis_protocol.h"



////////////////////////////////////////////////////////////////////////////////
// command function implementations (NULL = unimplemented)

typedef void (*redis_command_handler)(redis_socket*, redis_command*);

#define redis_command_AUTH   NULL
#define redis_command_BGREWRITEAOF   NULL

#define redis_command_BLPOP   NULL
#define redis_command_BRPOP   NULL
#define redis_command_BRPOPLPUSH   NULL
#define redis_command_CLIENT   NULL
#define redis_command_CONFIG   NULL
#define redis_command_FLUSHDB   NULL
#define redis_command_MONITOR   NULL
#define redis_command_MOVE   NULL
#define redis_command_PSUBSCRIBE   NULL
#define redis_command_PUBSUB   NULL
#define redis_command_PUBLISH   NULL
#define redis_command_PUNSUBSCRIBE   NULL
#define redis_command_SAVE   NULL
#define redis_command_SELECT   NULL
#define redis_command_SHUTDOWN   NULL
#define redis_command_SLOWLOG   NULL
#define redis_command_SUBSCRIBE   NULL
#define redis_command_UNSUBSCRIBE   NULL
#define redis_command_UNWATCH   NULL
#define redis_command_WATCH   NULL

#define redis_command_BITOP   NULL
#define redis_command_DISCARD   NULL
#define redis_command_EVAL   NULL
#define redis_command_EVALSHA   NULL
#define redis_command_EXEC   NULL
#define redis_command_MIGRATE   NULL
#define redis_command_MULTI   NULL
#define redis_command_RENAME   NULL
#define redis_command_RENAMENX   NULL
#define redis_command_RPOPLPUSH   NULL
#define redis_command_SCAN   NULL
#define redis_command_SCRIPT   NULL
#define redis_command_SDIFF   NULL
#define redis_command_SDIFFSTORE   NULL
#define redis_command_SINTER   NULL
#define redis_command_SINTERSTORE   NULL
#define redis_command_SLAVEOF   NULL
#define redis_command_SMOVE   NULL
#define redis_command_SUNION   NULL
#define redis_command_SUNIONSTORE   NULL
#define redis_command_SYNC   NULL
#define redis_command_ZINTERSTORE   NULL
#define redis_command_ZUNIONSTORE   NULL

#define redis_command_APPEND   NULL
#define redis_command_BITCOUNT   NULL
#define redis_command_DBSIZE   NULL
#define redis_command_DEBUG   NULL
#define redis_command_DECR   NULL
#define redis_command_DECRBY   NULL
#define redis_command_DEL   NULL
#define redis_command_DUMP   NULL
#define redis_command_ECHO   NULL
#define redis_command_EXISTS   NULL
#define redis_command_EXPIRE   NULL
#define redis_command_EXPIREAT   NULL
#define redis_command_FLUSHALL   NULL
#define redis_command_GET   NULL
#define redis_command_GETBIT   NULL
#define redis_command_GETRANGE   NULL
#define redis_command_GETSET   NULL
#define redis_command_HDEL   NULL
#define redis_command_HEXISTS   NULL
#define redis_command_HGET   NULL
#define redis_command_HGETALL   NULL
#define redis_command_HINCRBY   NULL
#define redis_command_HINCRBYFLOAT   NULL
#define redis_command_HKEYS   NULL
#define redis_command_HLEN   NULL
#define redis_command_HMGET   NULL
#define redis_command_HMSET   NULL
#define redis_command_HSCAN   NULL
#define redis_command_HSET   NULL
#define redis_command_HSETNX   NULL
#define redis_command_HVALS   NULL
#define redis_command_INCR   NULL
#define redis_command_INCRBY   NULL
#define redis_command_INCRBYFLOAT   NULL
#define redis_command_INFO   NULL
#define redis_command_KEYS   NULL
#define redis_command_LASTSAVE   NULL
#define redis_command_LINDEX   NULL
#define redis_command_LINSERT   NULL
#define redis_command_LLEN   NULL
#define redis_command_LPOP   NULL
#define redis_command_LPUSH   NULL
#define redis_command_LPUSHX   NULL
#define redis_command_LRANGE   NULL
#define redis_command_LREM   NULL
#define redis_command_LSET   NULL
#define redis_command_LTRIM   NULL
#define redis_command_MGET   NULL
#define redis_command_MSET   NULL
#define redis_command_MSETNX   NULL
#define redis_command_OBJECT   NULL
#define redis_command_PERSIST   NULL
#define redis_command_PEXPIRE   NULL
#define redis_command_PEXPIREAT   NULL

void redis_command_PING(redis_socket* sock, redis_command* cmd) {
  redis_send_string_response(sock, "PONG", 0);
}

#define redis_command_PSETEX   NULL
#define redis_command_PTTL   NULL
#define redis_command_QUIT   NULL
#define redis_command_RANDOMKEY   NULL
#define redis_command_RESTORE   NULL
#define redis_command_RPOP   NULL
#define redis_command_RPUSH   NULL
#define redis_command_RPUSHX   NULL
#define redis_command_SADD   NULL
#define redis_command_SCARD   NULL
#define redis_command_SCRIPT   NULL
#define redis_command_SET   NULL
#define redis_command_SETBIT   NULL
#define redis_command_SETEX   NULL
#define redis_command_SETNX   NULL
#define redis_command_SETRANGE   NULL
#define redis_command_SISMEMBER   NULL
#define redis_command_SMEMBERS   NULL
#define redis_command_SORT   NULL
#define redis_command_SPOP   NULL
#define redis_command_SRANDMEMBER   NULL
#define redis_command_SREM   NULL
#define redis_command_SSCAN   NULL
#define redis_command_STRLEN   NULL
#define redis_command_TIME   NULL
#define redis_command_TTL   NULL
#define redis_command_TYPE   NULL
#define redis_command_ZADD   NULL
#define redis_command_ZCARD   NULL
#define redis_command_ZCOUNT   NULL
#define redis_command_ZINCRBY   NULL
#define redis_command_ZRANGE   NULL
#define redis_command_ZRANGEBYSCORE   NULL
#define redis_command_ZRANK   NULL
#define redis_command_ZREM   NULL
#define redis_command_ZREMRANGEBYRANK   NULL
#define redis_command_ZREMRANGEBYSCORE   NULL
#define redis_command_ZREVRANGE   NULL
#define redis_command_ZREVRANGEBYSCORE   NULL
#define redis_command_ZREVRANK   NULL
#define redis_command_ZSCAN   NULL
#define redis_command_ZSCORE   NULL

// called when we don't know what the fuck is going on
void redis_command_default(redis_socket* sock, redis_command* cmd) {
  int x;
  printf("UNKNOWN COMMAND:");
  for (x = 0; x < cmd->num_args; x++)
    printf(" %.*s", cmd->args[x].size, (const char*)cmd->args[x].data);
  printf("\n");
  redis_send_string_response(sock, "ERR unknown command", 1);
}



////////////////////////////////////////////////////////////////////////////////
// command table & lookup functions

struct {
  const char* command_str;
  void* handler; // TODO right type
} command_definitions[] = {

  // commands that probably will never be implemented
  {"AUTH",              redis_command_AUTH}, // password - Authenticate to the server
  {"BGREWRITEAOF",      redis_command_BGREWRITEAOF}, // - Asynchronously rewrite the append-only file
  {"BLPOP",             redis_command_BLPOP}, // key [key ...] timeout - Remove and get the first element in a list, or block until one is available
  {"BRPOP",             redis_command_BRPOP}, // key [key ...] timeout - Remove and get the last element in a list, or block until one is available
  {"BRPOPLPUSH",        redis_command_BRPOPLPUSH}, // source destination timeout - Pop a value from a list, push it to another list and return it; or block until one is available
  {"CLIENT",            redis_command_CLIENT}, // KILL ip:port / LIST / GETNAME / SETNAME name
  {"CONFIG",            redis_command_CONFIG}, // GET parameter / REWRITE / SET param value / RESETSTAT
  {"FLUSHDB",           redis_command_FLUSHDB}, // - Remove all keys from the current database
  {"MONITOR",           redis_command_MONITOR}, // - Listen for all requests received by the server in real time
  {"MOVE",              redis_command_MOVE}, // key db - Move a key to another database
  {"PSUBSCRIBE",        redis_command_PSUBSCRIBE}, // pattern [pattern ...] - Listen for messages published to channels matching the given patterns
  {"PUBSUB",            redis_command_PUBSUB}, // subcommand [argument [argument ...]] - Inspect the state of the Pub/Sub subsystem
  {"PUBLISH",           redis_command_PUBLISH}, // channel message - Post a message to a channel
  {"PUNSUBSCRIBE",      redis_command_PUNSUBSCRIBE}, // [pattern [pattern ...]] - Stop listening for messages posted to channels matching the given patterns
  {"SAVE",              redis_command_SAVE}, // - Synchronously save the dataset to disk
  {"SELECT",            redis_command_SELECT}, // index - Change the selected database for the current connection
  {"SHUTDOWN",          redis_command_SHUTDOWN}, // [NOSAVE] [SAVE] - Synchronously save the dataset to disk and then shut down the server
  {"SLOWLOG",           redis_command_SLOWLOG}, // subcommand [argument] - Manages the Redis slow queries log
  {"SUBSCRIBE",         redis_command_SUBSCRIBE}, // channel [channel ...] - Listen for messages published to the given channels
  {"UNSUBSCRIBE",       redis_command_UNSUBSCRIBE}, // [channel [channel ...]] - Stop listening for messages posted to the given channels
  {"UNWATCH",           redis_command_UNWATCH}, // - Forget about all watched keys
  {"WATCH",             redis_command_WATCH}, // key [key ...] - Watch the given keys to determine execution of the MULTI/EXEC block

  // commands that are hard to implement
  {"BITOP",             redis_command_BITOP}, // operation destkey key [key ...] - Perform bitwise operations between strings
  {"DISCARD",           redis_command_DISCARD}, // - Discard all commands issued after MULTI
  {"EVAL",              redis_command_EVAL}, // script numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EVALSHA",           redis_command_EVALSHA}, // sha1 numkeys key [key ...] arg [arg ...] - Execute a Lua script server side
  {"EXEC",              redis_command_EXEC}, // - Execute all commands issued after MULTI
  {"MIGRATE",           redis_command_MIGRATE}, // host port key destination-db timeout [COPY] [REPLACE] - Atomically transfer a key from a Redis instance to another one.
  {"MULTI",             redis_command_MULTI}, // - Mark the start of a transaction block
  {"RENAME",            redis_command_RENAME}, // key newkey - Rename a key
  {"RENAMENX",          redis_command_RENAMENX}, // key newkey - Rename a key, only if the new key does not exist
  {"RPOPLPUSH",         redis_command_RPOPLPUSH}, // source destination - Remove the last element in a list, append it to another list and return it
  {"SCAN",              redis_command_SCAN}, // cursor [MATCH pattern] [COUNT count] - Incrementally iterate the keys space
  {"SCRIPT",            redis_command_SCRIPT}, // KILL / EXISTS name / FLUSH / LOAD data - Kill the script currently in execution.
  {"SDIFF",             redis_command_SDIFF}, // key [key ...] - Subtract multiple sets
  {"SDIFFSTORE",        redis_command_SDIFFSTORE}, // destination key [key ...] - Subtract multiple sets and store the resulting set in a key
  {"SINTER",            redis_command_SINTER}, // key [key ...] - Intersect multiple sets
  {"SINTERSTORE",       redis_command_SINTERSTORE}, // destination key [key ...] - Intersect multiple sets and store the resulting set in a key
  {"SLAVEOF",           redis_command_SLAVEOF}, // host port - Make the server a slave of another instance, or promote it as master
  {"SMOVE",             redis_command_SMOVE}, // source destination member - Move a member from one set to another
  {"SUNION",            redis_command_SUNION}, // key [key ...] - Add multiple sets
  {"SUNIONSTORE",       redis_command_SUNIONSTORE}, // destination key [key ...] - Add multiple sets and store the resulting set in a key
  {"SYNC",              redis_command_SYNC}, // - Internal command used for replication
  {"ZINTERSTORE",       redis_command_ZINTERSTORE}, // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Intersect multiple sorted sets and store the resulting sorted set in a new key
  {"ZUNIONSTORE",       redis_command_ZUNIONSTORE}, // destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] - Add multiple sorted sets and store the resulting sorted set in a new key

  // commands that are easy to implement
  {"APPEND",            redis_command_APPEND}, // key value - Append a value to a key
  {"BITCOUNT",          redis_command_BITCOUNT}, // key [start] [end] - Count set bits in a string
  {"DBSIZE",            redis_command_DBSIZE}, // - Return the number of keys in the selected database
  {"DEBUG",             redis_command_DEBUG}, // OBJECT key - Get debugging information about a key
  {"DECR",              redis_command_DECR}, // key - Decrement the integer value of a key by one
  {"DECRBY",            redis_command_DECRBY}, // key decrement - Decrement the integer value of a key by the given number
  {"DEL",               redis_command_DEL}, // key [key ...] - Delete a key
  {"DUMP",              redis_command_DUMP}, // key - Return a serialized version of the value stored at the specified key.
  {"ECHO",              redis_command_ECHO}, // message - Echo the given string
  {"EXISTS",            redis_command_EXISTS}, // key - Determine if a key exists
  {"EXPIRE",            redis_command_EXPIRE}, // key seconds - Set a keys time to live in seconds
  {"EXPIREAT",          redis_command_EXPIREAT}, // key timestamp - Set the expiration for a key as a UNIX timestamp
  {"FLUSHALL",          redis_command_FLUSHALL}, // - Remove all keys from all databases
  {"GET",               redis_command_GET}, // key - Get the value of a key
  {"GETBIT",            redis_command_GETBIT}, // key offset - Returns the bit value at offset in the string value stored at key
  {"GETRANGE",          redis_command_GETRANGE}, // key start end - Get a substring of the string stored at a key
  {"GETSET",            redis_command_GETSET}, // key value - Set the string value of a key and return its old value
  {"HDEL",              redis_command_HDEL}, // key field [field ...] - Delete one or more hash fields
  {"HEXISTS",           redis_command_HEXISTS}, // key field - Determine if a hash field exists
  {"HGET",              redis_command_HGET}, // key field - Get the value of a hash field
  {"HGETALL",           redis_command_HGETALL}, // key - Get all the fields and values in a hash
  {"HINCRBY",           redis_command_HINCRBY}, // key field increment - Increment the integer value of a hash field by the given number
  {"HINCRBYFLOAT",      redis_command_HINCRBYFLOAT}, // key field increment - Increment the float value of a hash field by the given amount
  {"HKEYS",             redis_command_HKEYS}, // key - Get all the fields in a hash
  {"HLEN",              redis_command_HLEN}, // key - Get the number of fields in a hash
  {"HMGET",             redis_command_HMGET}, // key field [field ...] - Get the values of all the given hash fields
  {"HMSET",             redis_command_HMSET}, // key field value [field value ...] - Set multiple hash fields to multiple values
  {"HSCAN",             redis_command_HSCAN}, // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate hash fields and associated values
  {"HSET",              redis_command_HSET}, // key field value - Set the string value of a hash field
  {"HSETNX",            redis_command_HSETNX}, // key field value - Set the value of a hash field, only if the field does not exist
  {"HVALS",             redis_command_HVALS}, // key - Get all the values in a hash
  {"INCR",              redis_command_INCR}, // key - Increment the integer value of a key by one
  {"INCRBY",            redis_command_INCRBY}, // key increment - Increment the integer value of a key by the given amount
  {"INCRBYFLOAT",       redis_command_INCRBYFLOAT}, // key increment - Increment the float value of a key by the given amount
  {"INFO",              redis_command_INFO}, // [section] - Get information and statistics about the server
  {"KEYS",              redis_command_KEYS}, // pattern - Find all keys matching the given pattern
  {"LASTSAVE",          redis_command_LASTSAVE}, // - Get the UNIX time stamp of the last successful save to disk  
  {"LINDEX",            redis_command_LINDEX}, // key index - Get an element from a list by its index
  {"LINSERT",           redis_command_LINSERT}, // key BEFORE|AFTER pivot value - Insert an element before or after another element in a list
  {"LLEN",              redis_command_LLEN}, // key - Get the length of a list
  {"LPOP",              redis_command_LPOP}, // key - Remove and get the first element in a list
  {"LPUSH",             redis_command_LPUSH}, // key value [value ...] - Prepend one or multiple values to a list
  {"LPUSHX",            redis_command_LPUSHX}, // key value - Prepend a value to a list, only if the list exists
  {"LRANGE",            redis_command_LRANGE}, // key start stop - Get a range of elements from a list
  {"LREM",              redis_command_LREM}, // key count value - Remove elements from a list
  {"LSET",              redis_command_LSET}, // key index value - Set the value of an element in a list by its index
  {"LTRIM",             redis_command_LTRIM}, // key start stop - Trim a list to the specified range
  {"MGET",              redis_command_MGET}, // key [key ...] - Get the values of all the given keys
  {"MSET",              redis_command_MSET}, // key value [key value ...] - Set multiple keys to multiple values
  {"MSETNX",            redis_command_MSETNX}, // key value [key value ...] - Set multiple keys to multiple values, only if none of the keys exist (just do an EXISTS everywhere first)
  {"OBJECT",            redis_command_OBJECT}, // subcommand [arguments [arguments ...]] - Inspect the internals of Redis objects
  {"PERSIST",           redis_command_PERSIST}, // key - Remove the expiration from a key
  {"PEXPIRE",           redis_command_PEXPIRE}, // key milliseconds - Set a keys time to live in milliseconds
  {"PEXPIREAT",         redis_command_PEXPIREAT}, // key milliseconds-timestamp - Set the expiration for a key as a UNIX timestamp specified in milliseconds
  {"PING",              redis_command_PING}, // - Ping the server
  {"PSETEX",            redis_command_PSETEX}, // key milliseconds value - Set the value and expiration in milliseconds of a key
  {"PTTL",              redis_command_PTTL}, // key - Get the time to live for a key in milliseconds
  {"QUIT",              redis_command_QUIT}, // - Close the connection
  {"RANDOMKEY",         redis_command_RANDOMKEY}, // - Return a random key from the keyspace
  {"RESTORE",           redis_command_RESTORE}, // key ttl serialized-value - Create a key using the provided serialized value, previously obtained using DUMP.
  {"RPOP",              redis_command_RPOP}, // key - Remove and get the last element in a list
  {"RPUSH",             redis_command_RPUSH}, // key value [value ...] - Append one or multiple values to a list
  {"RPUSHX",            redis_command_RPUSHX}, // key value - Append a value to a list, only if the list exists
  {"SADD",              redis_command_SADD}, // key member [member ...] - Add one or more members to a set
  {"SCARD",             redis_command_SCARD}, // key - Get the number of members in a set
  {"SET",               redis_command_SET}, // key value [EX seconds] [PX milliseconds] [NX|XX] - Set the string value of a key
  {"SETBIT",            redis_command_SETBIT}, // key offset value - Sets or clears the bit at offset in the string value stored at key
  {"SETEX",             redis_command_SETEX}, // key seconds value - Set the value and expiration of a key
  {"SETNX",             redis_command_SETNX}, // key value - Set the value of a key, only if the key does not exist
  {"SETRANGE",          redis_command_SETRANGE}, // key offset value - Overwrite part of a string at key starting at the specified offset
  {"SISMEMBER",         redis_command_SISMEMBER}, // key member - Determine if a given value is a member of a set
  {"SMEMBERS",          redis_command_SMEMBERS}, // key - Get all the members in a set
  {"SORT",              redis_command_SORT}, // key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination] - Sort the elements in a list, set or sorted set
  {"SPOP",              redis_command_SPOP}, // key - Remove and return a random member from a set
  {"SRANDMEMBER",       redis_command_SRANDMEMBER}, // key [count] - Get one or multiple random members from a set
  {"SREM",              redis_command_SREM}, // key member [member ...] - Remove one or more members from a set
  {"SSCAN",             redis_command_SSCAN}, // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate Set elements
  {"STRLEN",            redis_command_STRLEN}, // key - Get the length of the value stored in a key
  {"TIME",              redis_command_TIME}, // - Return the current server time
  {"TTL",               redis_command_TTL}, // key - Get the time to live for a key
  {"TYPE",              redis_command_TYPE}, // key - Determine the type stored at key
  {"ZADD",              redis_command_ZADD}, // key score member [score member ...] - Add one or more members to a sorted set, or update its score if it already exists
  {"ZCARD",             redis_command_ZCARD}, // key - Get the number of members in a sorted set
  {"ZCOUNT",            redis_command_ZCOUNT}, // key min max - Count the members in a sorted set with scores within the given values
  {"ZINCRBY",           redis_command_ZINCRBY}, // key increment member - Increment the score of a member in a sorted set
  {"ZRANGE",            redis_command_ZRANGE}, // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index
  {"ZRANGEBYSCORE",     redis_command_ZRANGEBYSCORE}, // key min max [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score
  {"ZRANK",             redis_command_ZRANK}, // key member - Determine the index of a member in a sorted set
  {"ZREM",              redis_command_ZREM}, // key member [member ...] - Remove one or more members from a sorted set
  {"ZREMRANGEBYRANK",   redis_command_ZREMRANGEBYRANK}, // key start stop - Remove all members in a sorted set within the given indexes
  {"ZREMRANGEBYSCORE",  redis_command_ZREMRANGEBYSCORE}, // key min max - Remove all members in a sorted set within the given scores
  {"ZREVRANGE",         redis_command_ZREVRANGE}, // key start stop [WITHSCORES] - Return a range of members in a sorted set, by index, with scores ordered from high to low
  {"ZREVRANGEBYSCORE",  redis_command_ZREVRANGEBYSCORE}, // key max min [WITHSCORES] [LIMIT offset count] - Return a range of members in a sorted set, by score, with scores ordered from high to low
  {"ZREVRANK",          redis_command_ZREVRANK}, // key member - Determine the index of a member in a sorted set, with scores ordered from high to low
  {"ZSCAN",             redis_command_ZSCAN}, // key cursor [MATCH pattern] [COUNT count] - Incrementally iterate sorted sets elements and associated scores
  {"ZSCORE",            redis_command_ZSCORE}, // key member - Get the score associated with the given member in a sorted set

  {NULL, NULL}, // end marker
};

#define MAX_COMMANDS_PER_HASH_BUCKET 8

int8_t hash_to_num_commands[256];
int16_t hash_to_command_indexes[256][MAX_COMMANDS_PER_HASH_BUCKET];

uint8_t hash_command(const char* cmd, int len) {
  uint8_t hash = 0;
  const char* end;
  for (end = cmd + len; cmd < end; cmd++)
    hash += toupper(*cmd);
  return hash;
}

int build_command_definitions() {
  memset(hash_to_command_indexes, 0, sizeof(hash_to_command_indexes));
  memset(hash_to_num_commands, 0, sizeof(hash_to_num_commands));

  int x;
  for (x = 0; command_definitions[x].command_str; x++) {
    if (!command_definitions[x].handler)
      continue;
    uint8_t hash = hash_command(command_definitions[x].command_str, strlen(command_definitions[x].command_str));
    if (hash_to_num_commands[hash] >= MAX_COMMANDS_PER_HASH_BUCKET) {
      printf("error: too many commands in bucket %02X\n", hash);
      return -1;
    } else {
      hash_to_command_indexes[hash][hash_to_num_commands[hash]] = x;
      hash_to_num_commands[hash]++;
    }
  }

#ifdef DEBUG_COMMAND_COLLISIONS
  int y;
  for (x = 0; x < 256; x++) {
    if (hash_to_num_commands[x] > 1) {
      printf("%02X has %d entries:", x, hash_to_num_commands[x]);
      for (y = 0; y < hash_to_num_commands[x]; y++)
        printf(" %s", command_definitions[hash_to_command_indexes[x][y]].command_str);
      printf("\n");
    }
  }
#endif

  return 0;
}

redis_command_handler handler_for_command(const char* command, int cmdlen) {

  uint8_t hash = hash_command(command, cmdlen);

  int x;
  for (x = 0; x < hash_to_num_commands[hash]; x++)
    if (!strcmp(command, command_definitions[hash_to_command_indexes[hash][x]].command_str))
      return command_definitions[hash_to_command_indexes[hash][x]].handler;
  return redis_command_default;
}



////////////////////////////////////////////////////////////////////////////////
// server functions

void redis_server_handle_command(redis_socket* sock, redis_command* cmd) {
  if (cmd->num_args <= 0) {
    redis_send_string_response(sock, "ERR invalid command", 1);
    return;
  }

  int x;
  char* arg0_str = (char*)cmd->args[0].data;
  for (x = 0; x < cmd->args[0].size; x++)
    arg0_str[x] = toupper(arg0_str[x]);

  redis_command_handler handler = handler_for_command(arg0_str, cmd->args[0].size);
  handler(sock, cmd);
}

void redis_server_thread(redis_socket* sock) {
  for (;;) {
    redis_command* cmd = redis_receive_command(sock, sock);
    redis_server_handle_command(sock, cmd);
    resource_delete_ref(sock, cmd);
  }
}
