package redisqueue

import (
	"sync/atomic"
	"time"
	"fmt"

	"github.com/weikaishio/go-logger/logger"
	"encoding/json"
	"errors"
	"hallversion/common/qqredis"
	"strings"
)

const (
	PullQueueBlockTime = 5
	PullQueueErrTime   = 1
)

type RedisQueue struct {
	redisclient qqredis.RedisCache
	deQueueName []string
	running     int32
	msgque      chan backmsg
}

type backmsg struct {
	msg     *QueueMsg
	queName string
}

func NewRedisQueue(rc qqredis.RedisCache, deQueueName string) *RedisQueue {

	redisqueue := &RedisQueue{}

	redisqueue.deQueueName = strings.Split(deQueueName, ",")

	redisqueue.running = 1

	redisqueue.redisclient = rc

	redisqueue.msgque = make(chan backmsg, 10)

	return redisqueue
}

func (q *RedisQueue) RedisCache() qqredis.RedisCache {
	return q.redisclient
}

func (q *RedisQueue) Quit() {
	logger.LogInfo("RedisQueue %s %s ready quit", q.deQueueName)
	atomic.StoreInt32(&q.running, 0)
	logger.LogInfo("RedisQueue %s %s quit ok", q.deQueueName)
}

//运行状态
func (q *RedisQueue) IsRunning() bool {
	return atomic.LoadInt32(&q.running) != 0
}

//入列
func (q *RedisQueue) EnQueue(model *QueueMsg, queuename string) error {
	bt, err := json.Marshal(model)
	if err != nil {
		return err
	}
	return q.redisclient.Lpush(queuename, bt)
}

//返回队列
func (q *RedisQueue) BackQueue(model *QueueMsg, queuename string) error {
	bt, err := json.Marshal(model)

	if err != nil {
		return err
	}

	return q.redisclient.Rpush(queuename, bt)
}

func (q *RedisQueue) GetQueueMsg() (*QueueMsg, string) {

	qmg := <-q.msgque

	return qmg.msg, qmg.queName
}

func (q *RedisQueue) Start() {

	if len(q.deQueueName) == 0 {
		return
	}

	for i := 0; i < len(q.deQueueName); i++ {

		fun := func(index int) {
			for {
				quename, msg, err := q.DeQueue(q.deQueueName[index])

				if err != nil || msg == nil {

					continue
				}

				mst := backmsg{
					queName: quename,
					msg:     msg,
				}

				q.msgque <- mst
			}
		}
		go fun(i)
	}
}

//出列
func (q *RedisQueue) DeQueue(queuename string) (string, *QueueMsg, error) {

	model := new(QueueMsg)
	qn := ""

	if q.IsRunning() {
		var err = errors.New("")
		var item = []byte{}
		qn, item, err = q.redisclient.BRpop(queuename, PullQueueBlockTime)

		if err != nil {

			if err.Error() != "redigo: nil returned" {
				logger.LogWarn(fmt.Sprintf("RedisQueue error: %v", err))
				time.Sleep(PullQueueErrTime * time.Second)
			}

			return "", nil, err
		}

		if item == nil {
			return "", nil, nil
		}

		err = json.Unmarshal(item, model)

		if err != nil {
			return "", nil, err
		}
	}
	return qn, model, nil
}
