package util

import (
	"fmt"
	"github.com/phuslu/log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestInitMqService(t *testing.T) {
	mqUrls := []string{"amqp://pigt:PIGT@127.0.0.1:5072/", "amqp://pigt:PIGT@127.0.0.1:5672/"}
	InitMqService(mqUrls, 2, 2)

	GateMsg()
	//BizMsg()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	signal.Notify(c, syscall.SIGHUP)
	<-c
}

func BizMsg() {
	mqService := GetMultiMqService()
	//biz
	topicExchange := "platform:msg"
	queue := fmt.Sprintf("%s:%s", topicExchange, "imBiz")

	bizDelivery, err := mqService.SubscribeMsg(topicExchange, queue, "")
	if err != nil {
		log.Warn().Msgf("fails to subscribe msg. err:%+v", err)
		return
	}

	go func() {
		for {
			select {
			case msg, ok := <-bizDelivery:
				if !ok {
					log.Warn().Msgf("delivery close")
					return
				}
				log.Debug().Msgf("receive msg. %+v", string(msg.Body))
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second * 5)
			msg := &PublishMsg{
				ExchangeName: topicExchange,
				RouteKey:     "",
				Data:         []byte(fmt.Sprintf("biz multi mq test. %+v", time.Now())),
			}
			mqService.PublishMsg(msg)
		}
	}()

}

func GateMsg() {
	mqService := GetMultiMqService()
	exchange := "im"
	gateId := "pigt1"
	subscribeQueue := fmt.Sprintf("%s:%s", "gate", gateId)
	delivery, err := mqService.SubscribeMsg(exchange, subscribeQueue, gateId)
	if err != nil {
		go func() {
			tick := time.NewTicker(time.Second * 5)
			select {
			case <-tick.C:
				{
					mqService.SubscribeMsg(exchange, subscribeQueue, gateId)
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case msg, ok := <-delivery:
				if !ok {
					return
				}
				log.Debug().Msgf("receive msg. %+v", string(msg.Body))
			}
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second * 5)
			msg := &PublishMsg{
				ExchangeName: exchange,
				RouteKey:     "pigt1",
				Data:         []byte(fmt.Sprintf("multi mq test. %d", time.Now().Second())),
			}
			mqService.PublishMsg(msg)
		}
	}()

}
