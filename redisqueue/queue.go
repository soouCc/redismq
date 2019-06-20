package redisqueue

import (
	"encoding/gob"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/satori/go.uuid"
	"github.com/weikaishio/go-logger/logger"
)

func init() {
	Register(&QueueMsg{})
}

// 注册对象用于序列化和反序列化
func Register(obj interface{}) {
	gob.Register(obj)
}

const (
	MsgTimeOut = 6000 //消息超时时间
	BkWeight   = 0    //消息处理失败是否重新加入队列，等级
)

//全局的消息队列客户端
var RQueueClient Queue

// 队列任务模型
type QueueMsg struct {
	UUID        string                 `json:"uuid"`        //uuid
	Msg         map[string]interface{} `json:"msg"`         //消息体
	Createdtime int64                  `json:"create_time"` //创建时间（毫秒）
	DeadTime    int64                  `json:"dead_time"`   //过期时间（毫秒） 当过期时间小于0时 为永不过期
	Weight      int                    `json:"weight"`      //消息的权重（主要用于处理消息处理失败后，该消息是否返回消息队列）
	Retry       int                    `json:"retry"`       //消息的重试次数
}

// 新建队列任务,会分配guid
func NewQueueMsg(msg map[string]interface{}, weight, retry int) (*QueueMsg, error) {
	uuid,_ := uuid.NewV4()

	guid := uuid.String()

	return &QueueMsg{
		UUID:        guid,
		Msg:         msg,
		Createdtime: time.Now().Unix(),
		DeadTime:    time.Now().Unix() + MsgTimeOut,
		Weight:      weight,
		Retry:       retry,
	}, nil
}

// 队列接口
type Queue interface {
	EnQueue(model *QueueMsg, queuename string) error
	DeQueue(queuename string) (string, *QueueMsg, error)
	BackQueue(model *QueueMsg, queuename string) error
	GetQueueMsg() (*QueueMsg, string)
	Quit()
	IsRunning() bool
	Start()
}

/**
	msg:消息体
	weight:消息权重
	retry:消息重试次数
	queuename:消息队列名称
**/
func EnQueueTask(q Queue, msg map[string]interface{}, weight, retry int, queuename string) (string, error) {
	m, err := NewQueueMsg(msg, weight, retry)
	if err != nil {
		return "", err
	}
	err = q.EnQueue(m, queuename)
	return m.UUID, err
}

//消息出列
func DeQueueTask(q Queue) (qm *QueueMsg, qname string, err error) {
	//会堵塞
	qm, qname = q.GetQueueMsg()
	if qm == nil || qm.Msg == nil {
		return nil, "", nil
	}

	//消息超时处理
	now := time.Now().Unix()
	if qm.DeadTime > 0 && now > qm.DeadTime {
		err = errors.New(fmt.Sprintf("Queue Error:TimeOut! Msg:", qm))

		return nil, "", err
	}

	return qm, qname, nil
}

// 消息返回队列
func BackQueueTask(q Queue, msg map[string]interface{}, weight, retry int, queuename string) (string, error) {

	m, err := NewQueueMsg(msg, weight, retry)
	if err != nil {
		return "", err
	}
	err = q.BackQueue(m, queuename)

	return m.UUID, err
}

//消息错误处理
func CatchError(q Queue, qm *QueueMsg, queuename string) {
	if err := recover(); err != nil {
		logger.LogError(fmt.Sprintf("CacheError, err:%v", err))
		logger.LogError(string(debug.Stack()))
	}

	if qm == nil {
		return
	}

	//如果消息比较重要，返回消息队列
	if qm.Weight >= BkWeight {

		if qm.Retry <= 0 {
			logger.LogError(fmt.Sprintf("消息从新进入队列失败！原因：尝试处理次数 Retry:%d，  Msg:%#v,", qm.Retry, *qm))
			return
		}

		qm.Retry--

		err := q.BackQueue(qm, queuename)
		logger.LogInfo(fmt.Sprintf("guid:%s 重新进入队列, :%v", qm.UUID))

		if err != nil {
			logger.LogError(fmt.Sprintf("重新进入队列失败！%v,guid:%s", err.Error(), qm.UUID))
		}
	}
}
