package util

import (
	"errors"
	"fmt"
	"github.com/phuslu/log"
	"github.com/streadway/amqp"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var multiMqService *MultiMqService

type MultiMqService struct {
	mqs []*SingleMq //multi rabbitmq
}

type MqConnection struct {
	connId              string
	conn                *amqp.Connection
	pubChannels         []*MqChannel
	closeChan           chan struct{} //close all channel listeners when connection disconnect
	consumeChannels     []*MqChannel  //one channel,one consumer
	consumeMux          sync.Mutex
	singleMq            *SingleMq
	mqUrl               string //i.e amqp://pigt:PIGT@127.0.0.1:5072/
	channelCountPerConn int
}

type MqChannel struct {
	channel       *amqp.Channel
	closeChan     chan struct{}
	subscribeInfo *SubscribeInfo //support one subscribe for one channel
}

type SingleMq struct {
	connections   []*MqConnection
	connectionMux sync.Mutex
	outChannel    chan *PublishMsg //for publishing msg
	outChannelOk  atomic.Value     //for publishing msg
}

type PublishMsg struct {
	ExchangeName string
	RouteKey     string
	Data         []byte
}

type SubscribeInfo struct {
	Exchange        string
	Queue           string
	RouteKey        string
	DeliveryChannel chan<- amqp.Delivery
}

func GetMultiMqService() *MultiMqService {
	return multiMqService
}

func InitMqService(mqUrls []string, connectionCont, channelCountPerConn int) error {
	if mqUrls == nil || len(mqUrls) < 1 || channelCountPerConn < 1 {
		return errors.New("wrong parameter")
	}

	multiMqService = &MultiMqService{
		mqs: make([]*SingleMq, 0),
	}

	for _, mqUrl := range mqUrls {
		sm := SingleMq{
			connections: make([]*MqConnection, 0),
			outChannel:  make(chan *PublishMsg, 100),
		}
		sm.outChannelOk.Store(false)
		multiMqService.mqs = append(multiMqService.mqs, &sm)
		for i := 0; i < connectionCont; i++ {
			mqConnection := sm.createSingleConnection(mqUrl, channelCountPerConn)
			if mqConnection == nil {
				log.Warn().Str("mq", mqUrl).Msgf("fails to create mq connection.")
				//continue to create connection, if fails to create
				go func(mq string, singleMq *SingleMq) {
					restoreConnection(&MqConnection{
						connId:              "",
						conn:                nil,
						pubChannels:         nil,
						consumeChannels:     nil,
						closeChan:           nil,
						singleMq:            singleMq,
						mqUrl:               mq,
						channelCountPerConn: channelCountPerConn,
					})
				}(mqUrl, &sm)
			}
		}
	}
	return nil
}

func (sm *SingleMq) createSingleConnection(mqUrl string, channelCountPerConn int) *MqConnection {
	conn, err := amqp.DialConfig(mqUrl, amqp.Config{
		ChannelMax: 0,
		Heartbeat:  time.Second * 5,
	})
	if err != nil {
		log.Warn().Msgf("fails to create connection to mq:%s,err:%s", mqUrl, err)
		return nil
	}

	mqConnection := &MqConnection{
		conn:                conn,
		connId:              GenUUID(),
		pubChannels:         make([]*MqChannel, 0),
		consumeChannels:     make([]*MqChannel, 0),
		closeChan:           make(chan struct{}),
		singleMq:            sm,
		mqUrl:               mqUrl,
		channelCountPerConn: channelCountPerConn,
	}

	for i := 0; i < channelCountPerConn; i++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Warn().Msgf("fails to create channel for connection:%+v", conn)
			break
		}
		err = ch.Confirm(false)
		if err != nil {
			log.Warn().Msgf("channel can't set no wait confirm. channel %+v", ch)
		}
		mqChannel := &MqChannel{
			channel:       ch,
			subscribeInfo: nil,
			closeChan:     make(chan struct{}),
		}
		mqConnection.pubChannels = append(mqConnection.pubChannels, mqChannel)
		log.Debug().Msgf("new mqConnection:%+v", mqConnection)

		go func(mqChannelTemp *MqChannel) {
			for err := range ch.NotifyClose(make(chan *amqp.Error)) {
				mqChannelTemp.closeChan <- struct{}{}
				log.Warn().Msgf("[channel close] connection:%+v,channel:%+v,err:%+v", mqConnection, ch, err)
			}
		}(mqChannel)
	}
	mqConnection.OnPublishMessage()
	go monitorConnection(mqConnection)
	sm.connectionMux.Lock()
	sm.connections = append(sm.connections, mqConnection)
	sm.outChannelOk.Store(true)
	sm.connectionMux.Unlock()
	return mqConnection
}

func (mqConnection *MqConnection) OnPublishMessage() {
	if mqConnection.pubChannels == nil || len(mqConnection.pubChannels) < 1 {
		return
	}
	for _, single := range mqConnection.pubChannels {
		go func(mqChannel *MqChannel) {
			for {
				select {
				case <-mqChannel.closeChan:
					return
				case <-mqConnection.closeChan:
					return
				case msg, ok := <-mqConnection.singleMq.outChannel:
					if !ok {
						return
					}
					err := mqChannel.channel.Publish(
						msg.ExchangeName,
						msg.RouteKey,
						false,
						false,
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "application/json",
							Body:         msg.Data,
						})
					//if err != nil {
					//	//发布有问题，channel损坏，退出订阅
					//	log.Warn().Msgf(fmt.Sprintf("fails to publish msg.mqConnection:%+v, %+v", mqConnection, err))
					//}

					log.Warn().Msgf(fmt.Sprintf("publish msg.mqConnection:%+v,err%+v", mqConnection, err))

				}
			}
		}(single)
	}
}

func (mm *MultiMqService) SubscribeMsg(exchange, queue, routeKey string) (<-chan amqp.Delivery, error) {
	if mm.mqs == nil || len(mm.mqs) < 1 {
		return nil, errors.New("no connection")
	}

	outChannel := make(chan amqp.Delivery)
	sub := &SubscribeInfo{
		Exchange:        exchange,
		Queue:           queue,
		RouteKey:        routeKey,
		DeliveryChannel: outChannel,
	}
	ok := false
	for _, singleMq := range mm.mqs {
		if singleMq.connections == nil || len(singleMq.connections) < 1 {
			continue
		}
		for _, singleConnection := range singleMq.connections {
			err := singleChannelSubscribe(singleConnection, sub)
			if err == nil {
				ok = true
			}
		}
	}
	if !ok {
		return nil, errors.New(fmt.Sprintf("fails to subscribe. exchange:%s, queue:%s, routeKey:%s", exchange, queue, routeKey))
	}
	return outChannel, nil
}

func singleChannelSubscribe(mqConnection *MqConnection, sub *SubscribeInfo) error {
	ch, err := GetChannel(mqConnection)
	if err != nil {
		return err
	}
	queue := sub.Queue
	routeKey := sub.RouteKey
	exchange := sub.Exchange

	//notice. channel will close if QueueDeclarePassive return err
	_, err = ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		ch, err = GetChannel(mqConnection)
		if err != nil {
			return err
		}
		_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			return err
		}
	}
	mqChannel := &MqChannel{
		channel:       ch,
		subscribeInfo: nil,
		closeChan:     make(chan struct{}),
	}
	//use separate channel for consume
	mqConnection.consumeMux.Lock()
	mqConnection.consumeChannels = append(mqConnection.consumeChannels, mqChannel)
	mqConnection.consumeMux.Unlock()

	if err := mqChannel.channel.QueueBind(queue, routeKey, exchange, false, nil); err != nil {
		return err
	}
	consumeTag := uniqueConsumerTag()
	deliveries, err := mqChannel.channel.Consume(queue, consumeTag, true, false, false, false, nil)
	if err != nil {
		return err
	}
	mqChannel.subscribeInfo = sub
	go func() {
		for v := range deliveries {
			log.Debug().Msgf("receive delivery msg:%s", string(v.Body))
			sub.DeliveryChannel <- v
		}
	}()
	go func(mqChannelTemp *MqChannel) {
		for err := range ch.NotifyClose(make(chan *amqp.Error)) {
			mqChannelTemp.closeChan <- struct{}{}
			log.Warn().Msgf("[channel close] connection:%+v,channel:%+v,err:%+v", mqConnection, ch, err)
		}
	}(mqChannel)
	return nil
}

func monitorConnection(connection *MqConnection) {
	log.Debug().Msgf(fmt.Sprintf("begin to monitor connection %+v", connection))
	select {
	case <-connection.conn.NotifyClose(make(chan *amqp.Error)):
		{
			connection.closeChan <- struct{}{}
			log.Debug().Msgf(fmt.Sprintf("connection close. %+v", connection))
			sm := connection.singleMq
			sm.connectionMux.Lock()
			sm.outChannelOk.Store(false)
			if sm.connections != nil && len(sm.connections) > 0 {
				connections := make([]*MqConnection, 0)
				for _, singleConnection := range sm.connections {
					if singleConnection.connId != connection.connId {
						connections = append(connections, singleConnection)
					}
				}
				sm.connections = connections
			}
			sm.connectionMux.Unlock()
			restoreConnection(connection)
		}
	}
}

func GetChannel(mqConnection *MqConnection) (*amqp.Channel, error) {
	ch, err := mqConnection.conn.Channel()
	if err != nil {
		log.Warn().Msgf("fails to create channel for connection:%+v", mqConnection.conn)
		return nil, err
	}
	err = ch.Confirm(false)
	if err != nil {
		log.Warn().Msgf("channel can't set no wait confirm. channel %+v", ch)
		return nil, err
	}
	return ch, nil
}

func restoreConnection(connection *MqConnection) {
	newConnection := connection.singleMq.createSingleConnection(connection.mqUrl, connection.channelCountPerConn)
	if newConnection == nil {
		//定时监听
		time.Sleep(time.Second * 5)
		restoreConnection(connection)
		return
	}

	oldConsumerChannels := connection.consumeChannels
	if oldConsumerChannels == nil || len(oldConsumerChannels) < 1 {
		return
	}

	//restore subscription
	for _, oldChannel := range oldConsumerChannels {
		sub := oldChannel.subscribeInfo
		if sub != nil {
			singleChannelSubscribe(newConnection, sub)
		}
	}
}

func (mm *MultiMqService) PublishMsg(msg *PublishMsg) {
	if mm.mqs == nil || len(mm.mqs) < 1 {
		return
	}
	for _, single := range mm.mqs {
		if single.outChannelOk.Load().(bool) {
			single.outChannel <- msg
			break
		}
	}
}

func uniqueConsumerTag() string {
	return commandNameBasedUniqueConsumerTag(os.Args[0])
}

var consumerSeq uint64

const consumerTagLengthMax = 0xFF // see writeShortstr

func commandNameBasedUniqueConsumerTag(commandName string) string {
	tagPrefix := "ctag-"
	tagInfix := commandName
	tagSuffix := "-" + strconv.FormatUint(atomic.AddUint64(&consumerSeq, 1), 10)

	if len(tagPrefix)+len(tagInfix)+len(tagSuffix) > consumerTagLengthMax {
		tagInfix = "streadway/amqp"
	}

	return tagPrefix + tagInfix + tagSuffix
}
