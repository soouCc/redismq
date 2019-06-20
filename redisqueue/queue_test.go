package redisqueue

import (
	"testing"
	"time"
	"fmt"

	"github.com/weikaishio/go-logger/logger"

	"hallversion/common/qqredis"
	"hallversion/common"
)

const (
	platform_dequeuename = "formplatform"
	platform_enqueuename = "formgame"

	other_dequeuename = "otherqueue"
	other_enqueuename = "otherqueue"

	game_dequeue = "game_dequeue"

	//addr        = "101.201.198.69:6479"
	//pw          = "redis_Nabai"
	addr        = "127.0.0.1:6379"
	pw          = ""
)

var rQueue *RedisQueue

func TestQueue(t *testing.T) {

	qc := qqredis.NewRedisCache(addr, pw, 1000)

	//q1 :=[]string{platform_dequeuename,other_dequeuename,game_dequeue}
	q2 := platform_dequeuename+","+other_dequeuename+","+game_dequeue

	rQueue = NewRedisQueue(qc,q2)
	rQueue.Start()

	logger.LogInfo(fmt.Sprintf("Queue:%#v  ,rc:%#v", rQueue, qc))
	time.Sleep(time.Second)

	go enqueuet()
	go enqueuet2()
	go enqueuet3()

	go startQueue()

	for {
		time.Sleep(100 * time.Second)
	}
}

func startQueue() {

	defer common.CacheError()
	var qm *QueueMsg
	var err error
	var qname string

	fn := func() {
		defer CatchError(rQueue,qm,qname)
		for {

			qm,qname, err = DeQueueTask(rQueue)
			if err != nil || qm == nil {
				continue
			}
			logger.LogInfo(fmt.Sprintf("qm::;>>>>>>>:%#v,***********\n%s", qm,qname))

		}
	}
	go fn()
}

func enqueuet() {

	defer common.CacheError()
	i := 0
	num := 100
	for {
		i++
		msg := map[string]interface{}{
			"cmd":   "create_table",
			"index": i,
			"queue":"11111111",
		}
		_, err := EnQueueTask(rQueue, msg, 2,1,platform_dequeuename)

		if err != nil {
			logger.LogInfo(err.Error())
		}

		if i%num==0{
			time.Sleep(2*time.Second)
		}
	}
}

func enqueuet2() {

	defer common.CacheError()
	i := 0
	num := 100
	for {
		i++
		msg := map[string]interface{}{
			"cmd":   "create_table",
			"index": i,
			"queue":"2222222",
		}
		_, err := EnQueueTask(rQueue, msg, 2,1,other_dequeuename)

		if err != nil {
			logger.LogInfo(err.Error())
		}

		if i%num==0{
			time.Sleep(2*time.Second)
		}
	}
}

func enqueuet3() {

	defer common.CacheError()
	i := 0
	num := 100
	for {
		i++
		msg := map[string]interface{}{
			"cmd":   "create_table",
			"index": i,
			"queue":"333333333",
		}
		_, err := EnQueueTask(rQueue, msg, 2,1,game_dequeue)

		if err != nil {
			logger.LogInfo(err.Error())
		}

		if i%num==0{
			time.Sleep(2*time.Second)
		}
	}
}