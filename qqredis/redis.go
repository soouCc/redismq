package qqredis

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	// "github.com/robfig/config"
	"errors"
	"time"
)

//改自revel框架的rediscache
//http://redis.readthedocs.org/en/2.6/
var (
	ErrCacheMiss = errors.New("rediscache: key not found.")
	ErrNotStored = errors.New("rediscache: not stored.")
	DEFAULT      = time.Duration(0)
	FOREVER      = time.Duration(-1)
)

// Wraps the Redis client to meet the Cache interface.
type RedisCache struct {
	pool              *redis.Pool
	defaultExpiration time.Duration
}
type Getter interface {
	// Get the content associated with the given key. decoding it into the given
	// pointer.
	//
	// Returns:
	//   - nil if the value was successfully retrieved and ptrValue set
	//   - ErrCacheMiss if the value was not in the cache
	//   - an implementation specific error otherwise
	Get(key string, ptrValue interface{}) error
}

// until redigo supports sharding/clustering, only one host will be in hostList
func NewRedisCache(host string, password string, defaultExpiration time.Duration,db int) RedisCache {
	var pool = &redis.Pool{
		MaxIdle:     5,
		MaxActive:   0,
		IdleTimeout: time.Duration(240) * time.Second,
		Dial: func() (redis.Conn, error) {
			protocol := "tcp"
			toc := time.Millisecond * time.Duration(10000)
			tor := time.Millisecond * time.Duration(500000)
			tow := time.Millisecond * time.Duration(5000)
			c, err := redis.DialTimeout(protocol, host, toc, tor, tow)

			if err != nil {
				return nil, err
			}

			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			} else {
				// check with PING
				if _, err := c.Do("PING"); err != nil {
					c.Close()
					return nil, err
				}
			}

			if db!=0{
				c.Do("SELECT",db)
			}
			return c, err
		},
		// custom connection test method
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if _, err := c.Do("PING"); err != nil {
				return err
			}
			return nil
		},
	}
	return RedisCache{pool, defaultExpiration}
}

func (c RedisCache) Set(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	return c.invoke(conn.Do, key, value, expires)
}
func (c RedisCache) Add(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := exists(conn, key)
	if err != nil {
		return err
	} else if existed {
		return ErrNotStored
	}
	return c.invoke(conn.Do, key, value, expires)
}

func (c RedisCache) Exists(key string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}

func (c RedisCache) TTL(key string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("ttl", key))
}

func (c RedisCache) Replace(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := exists(conn, key)
	if err != nil {
		return err
	} else if !existed {
		return ErrNotStored
	}
	err = c.invoke(conn.Do, key, value, expires)
	if value == nil {
		return ErrNotStored
	} else {
		return err
	}
}

func (c RedisCache) Get(key string, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("GET", key)
	if err != nil {
		return err
	} else if raw == nil {
		return ErrCacheMiss
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return Deserialize(item, ptrValue)
}
func (c RedisCache) GetStr(key string) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("GET", key)
	if err != nil {
		return "", err
	} else if raw == nil {
		return "", ErrCacheMiss
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return "", err
	}
	return string(item), nil
}
func generalizeStringSlice(strs []string) []interface{} {
	ret := make([]interface{}, len(strs))
	for i, str := range strs {
		ret[i] = str
	}
	return ret
}
func (c RedisCache) GetMulti(keys ...string) (Getter, error) {
	conn := c.pool.Get()
	defer conn.Close()

	items, err := redis.Values(conn.Do("MGET", generalizeStringSlice(keys)...))
	if err != nil {
		return nil, err
	} else if items == nil {
		return nil, ErrCacheMiss
	}

	m := make(map[string][]byte)
	for i, key := range keys {
		m[key] = nil
		if i < len(items) && items[i] != nil {
			s, ok := items[i].([]byte)
			if ok {
				m[key] = s
			}
		}
	}
	return RedisItemMapGetter(m), nil
}

func exists(conn redis.Conn, key string) (bool, error) {
	return redis.Bool(conn.Do("EXISTS", key))
}

func (c RedisCache) Delete(key string) error {
	conn := c.pool.Get()
	defer conn.Close()
	existed, err := redis.Bool(conn.Do("DEL", key))
	if err == nil && !existed {
		err = ErrCacheMiss
	}
	return err
}

//自增
func (c RedisCache) Increment(key string, delta uint64) (uint64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that. Since we need to do increment
	// ourselves instead of natively via INCRBY (redis doesn't support wrapping), we get the value
	// and do the exists check this way to minimize calls to Redis
	val, err := conn.Do("GET", key)
	if err != nil {
		return 0, err
	} else if val == nil {
		return 0, ErrCacheMiss
	}
	currentVal, err := redis.Int64(val, nil)
	if err != nil {
		return 0, err
	}
	var sum int64 = currentVal + int64(delta)
	_, err = conn.Do("SET", key, sum)
	if err != nil {
		return 0, err
	}
	return uint64(sum), nil
}

//自减
func (c RedisCache) Decrement(key string, delta uint64) (newValue uint64, err error) {
	conn := c.pool.Get()
	defer conn.Close()
	// Check for existance *before* increment as per the cache contract.
	// redis will auto create the key, and we don't want that, hence the exists call
	existed, err := exists(conn, key)
	if err != nil {
		return 0, err
	} else if !existed {
		return 0, ErrCacheMiss
	}
	// Decrement contract says you can only go to 0
	// so we go fetch the value and if the delta is greater than the amount,
	// 0 out the value
	currentVal, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		return 0, err
	}
	if delta > uint64(currentVal) {
		tempint, err := redis.Int64(conn.Do("DECRBY", key, currentVal))
		return uint64(tempint), err
	}
	tempint, err := redis.Int64(conn.Do("DECRBY", key, delta))
	return uint64(tempint), err
}

//关闭
func (c RedisCache) Flush() error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("FLUSHALL")
	return err
}

func (c RedisCache) invoke(f func(string, ...interface{}) (interface{}, error),
	key string, value interface{}, expires time.Duration) error {

	switch expires {
	case DEFAULT:
		expires = c.defaultExpiration
	case FOREVER:
		expires = time.Duration(0)
	}

	b, err := Serialize(value)
	if err != nil {
		return err
	}
	conn := c.pool.Get()
	defer conn.Close()
	if expires > 0 {
		_, err := f("SETEX", key, int32(expires/time.Second), b)
		return err
	} else {
		_, err := f("SET", key, b)
		return err
	}
}

// Implement a Getter on top of the returned item map.
type RedisItemMapGetter map[string][]byte

func (g RedisItemMapGetter) Get(key string, ptrValue interface{}) error {
	item, ok := g[key]
	if !ok {
		return ErrCacheMiss
	}
	if len(item) == 0 {
		return ErrCacheMiss
	}
	return Deserialize(item, ptrValue)
}

/*
redis queue model
LPUSH source value
BRPOPLPUSH source distination 300s-> value
ExecEnd then LREM distination 1 value
*/
func (c RedisCache) Lpush(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return err
	//	}
	_, err := conn.Do("LPUSH", key, value)
	return err
}

func (c RedisCache) BRpopLpush(key, bkkey string, second int, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("BRPOPLPUSH", key, bkkey, second)
	if err != nil {
		return err
	} else if raw == nil {
		return nil
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return Deserialize(item, ptrValue)
}
func (c RedisCache) Lpop(key string) error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("LPOP", key)
	// if isaffected == 0 {
	// 	// fmt.Println("LPOP:", key, isaffected, ",err:", err)
	// }
	return err
}
func (c RedisCache) Lrange(key string, begin int, end int) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("LRANGE", key, begin, end)
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
}
func (c RedisCache) Llen(key string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("LLEN", key)
	if err != nil {
		return 0, err
	} else if raw == nil {
		return 0, nil
	}
	return redis.Int(raw, err)
}
func (c RedisCache) Rpop(key string, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raw, err := conn.Do("RPOP", key)
	if err != nil {
		return err
	} else if raw == nil {
		return nil
	}
	item, err := redis.Bytes(raw, err)
	if err != nil {
		return err
	}
	return Deserialize(item, ptrValue)
}
func (c RedisCache) Lrem(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return err
	//	}
	isaffected, err := conn.Do("LREM", key, 1, value)
	if isaffected == 0 {
		// fmt.Println("LREM:", key, isaffected, ",err:", err, ",b:", b)
	}
	return err
}
func (c RedisCache) Sadd(key string, value interface{}) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Sadd", key, value)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}
func (c RedisCache) Sadds(field_value ...interface{}) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Sadd", field_value...)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}
func (c RedisCache) Sismember(key string, value interface{}) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Sismember", key, value)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Smembers(key string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	return redis.Strings(raws, err)
}

func (c RedisCache) SmembersInterface(key string, ptrValue interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("SMEMBERS", key)
	if err != nil {
		return err
	}

	item, err := redis.Bytes(raws, err)
	if err != nil {
		return err
	}

	return Deserialize(item, ptrValue)
}

func (c RedisCache) Srem(key string, value interface{}) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Srem", key, value)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}
func (c RedisCache) Scard(key string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Scard", key)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Spop(key string) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Spop", key)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}

/*sortedset*/
func (c RedisCache) Zadd(key string, value interface{}, score int64) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	isadd := false
	raws, err := conn.Do("ZRANGEBYSCORE", key, score, score)
	if err != nil {
		return isadd, err
	}
	switch raws := raws.(type) {
	case []interface{}:
		if len(raws) > 0 {
			return isadd, nil
		}
	}
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return isadd, err
	//	}
	isadd = true
	reply, err := conn.Do("ZADD", key, score, value)
	str, err := redis.String(reply, err)
	return str == "1", err
}
func (c RedisCache) ZaddJson(key string, value []byte, score int64) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("ZADD", key, score, value)
	if err != nil {
		return false, err
	}
	return true, err
}
func (c RedisCache) ZaddUpgrade(key string, value []byte, score int64) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZRANGEBYSCORE", key, score, score)
	if err != nil {
		return false, err
	}
	switch raws := raws.(type) {
	case []interface{}:
		if len(raws) > 0 {
			return false, nil
		}
	}
	_, err = conn.Do("ZADD", key, score, value)
	if err != nil {
		return false, err
	}
	return true, err
}
func (c RedisCache) Zrangewithscores(key string, begin, end int) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZRANGE", key, begin, end, "WITHSCORES")
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
}
func (c RedisCache) ZaddMultiScore(key string, value interface{}, score int64) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return false, err
	//	}
	_, err := conn.Do("ZADD", key, score, value)
	if err != nil {
		return false, err
	}
	return true, err
}
func (c RedisCache) Zincrby(key, member string, inc int) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(member)
	//	if err != nil {
	//		return 0, err
	//	}
	raws, err := conn.Do("ZINCRBY", key, inc, member)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Zscore(key string, member interface{}) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(member)
	//	if err != nil {
	//		return 0, err
	//	}
	raws, err := conn.Do("ZSCORE", key, member)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}

func (c RedisCache) Zscoreforstring(key string, member string) (float64, error) {
	conn := c.pool.Get()
	defer conn.Close()

	raws, err := conn.Do("ZSCORE", key, member)
	if err != nil {
		return 0, err
	}
	return redis.Float64(raws, err)
}

func (c RedisCache) ZscoreJson(key string, value []byte) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZSCORE", key, value)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) ZscoreInt(key string, member int) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZSCORE", key, member)
	if err != nil {
		return 0, err
	}

	return redis.Int(raws, err)
}
func (c RedisCache) Zrank(key string, value interface{}) int {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return -1
	//	}
	raws, _ := conn.Do("Zrank", key, value)
	rankInt := 0
	if raws == nil {
		rankInt = -1
	} else {
		rankInt = int(raws.(int64))
	}

	return rankInt
}
func (c RedisCache) Zrevrank(key string, value interface{}) int {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return -1
	//	}
	raws, _ := conn.Do("Zrevrank", key, value)
	rankInt := 0
	if raws == nil {
		rankInt = -1
	} else {
		rankInt = int(raws.(int64))
	}

	return rankInt
}

func (c RedisCache) Zrevrankforstring(key string, value string) int {
	conn := c.pool.Get()
	defer conn.Close()

	raws, _ := conn.Do("Zrevrank", key, value)
	rankInt := 0
	if raws == nil {
		rankInt = -1
	} else {
		rankInt = int(raws.(int64))
	}

	return rankInt
}

func (c RedisCache) ZremJson(key string, value []byte) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	reply, zremerr := conn.Do("ZREM", key, value)
	str, zremerr := redis.String(reply, zremerr)
	return str == "1", zremerr
}
func (c RedisCache) Zrem(key string, value interface{}) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return false, err
	//	}
	reply, zremerr := conn.Do("ZREM", key, value)
	str, zremerr := redis.String(reply, zremerr)
	return str == "1", zremerr
}
func (c RedisCache) Zrembyscore(key string, begin int64, end int64) error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("ZREMRANGEBYSCORE", key, begin, end)
	return err
}
func (c RedisCache) Zrembyrank(key string, begin int64, end int64) error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("ZREMRANGEBYRANK", key, begin, end)
	return err
}
func (c RedisCache) Zrangebyscore(key string, begin int64, end int64) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZRANGEBYSCORE", key, begin, end)
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
}
func (c RedisCache) Zrangebyscoreforstring(key string, begin int64, end int64) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZRANGEBYSCORE", key, begin, end)
	if err != nil {
		return nil, err
	}
	objAry := make([]string, 0)
	for _, raw := range raws.([]interface{}) {
		item, err := redis.Bytes(raw, err)
		if err != nil {
			continue
		}
		objAry = append(objAry, string(item))
	}
	return objAry, nil
}
func (c RedisCache) Zrangeforint(key string, begin, end int) ([]int, error) {
	raws, err := c.Zrange(key, begin, end)
	if err != nil {
		return nil, err
	}

	objAry := make([]int, 0)
	for _, raw := range raws {
		item, err := redis.Bytes(raw, err)
		if err != nil {
			continue
		}
		obj := 0
		Deserialize(item, &obj)
		if obj != 0 {
			objAry = append(objAry, obj)
		}
	}
	return objAry, nil
}
func (c RedisCache) Zrangeforstring(key string, begin, end int) ([]string, error) {
	raws, err := c.Zrange(key, begin, end)
	if err != nil {
		return nil, err
	}

	objAry := make([]string, 0)
	for _, raw := range raws {
		item, err := redis.Bytes(raw, err)
		if err != nil {
			continue
		}
		objAry = append(objAry, string(item))
	}
	return objAry, nil
}
func (c RedisCache) Zrange(key string, begin int, end int) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZRANGE", key, begin, end)
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
	// switch raws := raws.(type) {
	// case []interface{}:
	// 	return raws, nil
	// default:
	// 	return nil, nil
	// }
}
func (c RedisCache) Zrevrange(key string, begin int, end int) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZREVRANGE", key, begin, end)
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
}
func (c RedisCache) ZrevrangeByScores(key string, begin, end int) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZREVRANGEBYSCORE", key, begin, end)
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
}
func (c RedisCache) Zrevrangewithscores(key string, begin int, end int) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZREVRANGE", key, begin, end, "WITHSCORES")
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), nil
}
func (c RedisCache) Zcount(key string, minScore, maxScore int64) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZCOUNT", key, minScore, maxScore)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Zcard(key string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("ZCARD", key)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}

//hash
func (c RedisCache) Hgetall(key string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("HGETALL", key)
	if err != nil {
		return nil, err
	}
	return redis.Strings(raws, err)
}
func (c RedisCache) Hmget(key_field ...string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hmget", generalizeStringSlice(key_field)...)
	if err != nil {
		return nil, err
	}
	return redis.Strings(raws, err)
}
func (c RedisCache) Hmget2(key, field1, field2 string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hmget", key, field1, field2)
	if err != nil {
		return nil, err
	}
	return redis.Strings(raws, err)
}
func (c RedisCache) Hget(key, field string) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hget", key, field)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}
func (c RedisCache) Hdel(key, field string) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hdel", key, field)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}
func (c RedisCache) Hincrby(key, field string, count int) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("HINCRBY", key, field, count)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) HsetMulti(field_value ...interface{}) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()

	raws, err := conn.Do("Hmset", field_value...)
	if err != nil {
		return "", err
	}
	return redis.String(raws, err)
}
func (c RedisCache) HgetallObj(key string) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("HGETALL", key)
	if err != nil {
		return nil, err
	}
	return raws.([]interface{}), err
}
func (c RedisCache) HgetObj(key, field string) (interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hget", key, field)
	if err != nil {
		return "", err
	}
	return raws, err
}
func (c RedisCache) HsetObj(key, field string, value interface{}) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return 0, err
	//	}
	raws, err := conn.Do("Hset", key, field, value)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Hset(key, field string, value int) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hset", key, field, value)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Hsetstring(key, field, value string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Hset", key, field, value)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Expire(key string, expire int) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("EXPIRE", key, expire)
	if err != nil {
		return 0, err
	}
	return redis.Int(raws, err)
}
func (c RedisCache) Keys(key string) ([]string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("Keys", key)
	if err != nil {
		return nil, err
	}
	return redis.Strings(raws, err)
}

/*
Publish and Subscribe

Use the Send, Flush and Receive methods to implement Pub/Sub subscribers.

c.Send("SUBSCRIBE", "example")
c.Flush()
for {
    reply, err := c.Receive()
    if err != nil {
        return err
    }
    // process pushed message
}

The PubSubConn type wraps a Conn with convenience methods for implementing subscribers. The Subscribe, PSubscribe, Unsubscribe and PUnsubscribe methods send and flush a subscription management command. The receive method converts a pushed message to convenient types for use in a type switch.

psc := redis.PubSubConn{c}
psc.Subscribe("example")
for {
    switch v := psc.Receive().(type) {
    case redis.Message:
        fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
    case redis.Subscription:
        fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
    case error:
        return v
    }
}

*/
func (c RedisCache) Subscribe(key string, exec func(string, error)) {
	conn := c.pool.Get()
	defer conn.Close()
	conn.Send("SUBSCRIBE", key)
	conn.Flush()
	for {
		reply, err := conn.Receive()
		if err != nil {
			fmt.Println("Subscribe conn.Receive() err", key, err)
			return
		}
		for _, raw := range reply.([]interface{}) {
			item, err := redis.Bytes(raw, err)
			if err != nil {
				fmt.Println("Subscribe redis.Bytes err", key, err)
				continue
			}
			obj := ""
			err = Deserialize(item, &obj)
			if err != nil {
				fmt.Println("Subscribe Deserialize err", key, err)
			}
			exec(obj, err)
		}
	}
}
func (c RedisCache) Publish(key, value string) error {
	conn := c.pool.Get()
	defer conn.Close()

	//	b, err := Serialize(value)
	//	if err != nil {
	//		return err
	//	}
	err := conn.Send("PUBLISH", key, value)
	return err
}

// func (c RedisCache) Zrange(key string, begin int, end int, valuetype interface{}) ([]interface{}, error) {
// 	conn := c.pool.Get()
// 	defer conn.Close()
// 	raws, err := conn.Do("ZRANGE", key, begin, end)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// if v := reflect.ValueOf(ptrValue); v.Kind() == reflect.Ptr {
// 	// 	p := v.Elem()
// 	// 	fmt.Println("p.Kind():", p.Kind(), "reflect.TypeOf(ptrvalue):", reflect.TypeOf(ptrValue).Elem().Elem())
// 	// }
// 	switch raws := raws.(type) {
// 	case []interface{}:
// 		// switch ptype := reflect.TypeOf(ptrValue).Elem().Elem().(type) {
// 		// case interface{}:
// 		// 	fmt.Println("ptype:", ptype)
// 		// 	// ptrValue = make([]ptype, len(raws))
// 		// 	user := new(ptype)
// 		// 	ptrValue
// 		// 	fmt.Println("user:", reflect.TypeOf(user))
// 		// 	return nil, nil
// 		// default:
// 		// 	return nil, nil
// 		// }
// 		// fmt.Println("len(raws):", len(raws))

// 		res := make([]interface{}, len(raws))
// 		for i, raw := range raws {
// 			// fmt.Println("raw.type:", reflect.TypeOf(raw))
// 			item, err1 := redis.Bytes(raw, err)
// 			if err1 != nil {
// 				fmt.Println("redis.Bytes error:%v", err1)
// 			}
// 			Deserialize(item, valuetype)
// 			res[i] = valuetype
// 		}
// 		return res, nil
// 	default:
// 		return nil, nil
// 	}
// }

func (c RedisCache) Rpush(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(value)
	//	if err != nil {
	//		return err
	//	}
	_, err := conn.Do("RPUSH", key, value)
	return err
}

func (c RedisCache) BRpop(key string, time int) (string, []byte, error) {

	conn := c.pool.Get()
	defer conn.Close()

	raw, err := redis.Strings(conn.Do("BRPop", key, time))

	if err != nil {
		return "", nil, err
	} else if raw == nil {
		return "", nil, err
	} else if len(raw) != 2 {
		return "", nil, err
	}
	item, err := redis.Bytes(raw[1], err)
	queuename := raw[0]

	if err != nil {
		return "", nil, err
	}

	return queuename, item, err
}

func (c RedisCache) Zincrbyfloat64(key, member string, inc float64) (float64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	//	b, err := Serialize(member)
	//	if err != nil {
	//		return 0, err
	//	}
	raws, err := conn.Do("ZINCRBY", key, inc, member)
	if err != nil {
		return 0, err
	}
	return redis.Float64(raws, err)
}

func (c RedisCache) Hincrbyfloat64(key, field string, count float64) (float64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	raws, err := conn.Do("HINCRBYFLOAT", key, field, count)
	if err != nil {
		return 0, err
	}
	return redis.Float64(raws, err)
}

