package service

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/stan.go"
)

// EventBatchStreamingService сервис, позволяющий читать данные из NatsStreaming
// и обработывать сообщения пачками
type EventBatchStreamingService struct {
	sync.Mutex
	// уникальное имя сервиса (например clickhouse-loader)
	id           string
	wg           *sync.WaitGroup
	connLock     *sync.RWMutex
	conn         stan.Conn
	streamConfig []SubscriptionConfig
	streams      map[string]*EventBatchStream
	handlers     map[string]StreamingHandler
	shutdownCh   chan struct{}
	closeLock    *sync.Mutex
	stopCh       chan struct{}
	logger       *log.Logger
	isStopping   bool
}

type EventBatchStream struct {
	subscription stan.Subscription
	opts         *StreamingOptions
	// канал куда поступают сообщения из NatsStreaming
	msgsCh chan *stan.Msg
	// канал сигнализирующий обработчику сообщений о прекращении чтения сообщений
	stopCh chan struct{}
}

// StreamingOptions настройки сервиса потоковой обработки сообщений
type StreamingOptions struct {
	// Limit максимальное кол-во сообщений, которые одновременно находятся в обработке
	Limit int

	// Timeout время между циклами обработки
	Timeout time.Duration

	// AckWait максимальное время на чтение сообщения обработчиком
	// по истечению этого времени сообщение вернется обратно в очередь
	AckWait time.Duration

	DurableName string
	QueueGroup  string

	// DeadLetterSubject канал NatsStreaming, используемый для обработки ошибок
	DeadLetterSubject string

	// EventHandler обработчик, реализующий интерфейс для обработки сообщений
	EventHandler StreamingHandler
}

type StreamingHandler interface {
	Handle([]*stan.Msg) error
}

func NewEventBatchStreamingService(conf NatsStreamingConfig, logger *log.Logger) (*EventBatchStreamingService, error) {

	sr := &EventBatchStreamingService{
		id:           conf.ClientID,
		wg:           &sync.WaitGroup{},
		connLock:     &sync.RWMutex{},
		streamConfig: conf.Subscriptions,
		streams:      make(map[string]*EventBatchStream),
		handlers:     make(map[string]StreamingHandler),
		shutdownCh:   make(chan struct{}),
		stopCh:       make(chan struct{}, 1),
		closeLock:    &sync.Mutex{},
		logger:       logger,
	}

	if err := sr.Connect(conf); err != nil {
		return nil, err
	}

	return sr, nil
}

func (s *EventBatchStreamingService) logDebug(msg string, args ...interface{}) {
	s.logger.Printf("[DEBUG] (%s) - %s\n", s.id, fmt.Sprintf(msg, args...))
}

func (s *EventBatchStreamingService) logError(msg string, args ...interface{}) {
	s.logger.Printf("[ERROR] (%s) - %s\n", s.id, fmt.Sprintf(msg, args...))
}

func (s *EventBatchStreamingService) handleMessages(messages []*stan.Msg, opts *StreamingOptions) error {

	err := opts.EventHandler.Handle(messages)
	if err == nil {
		for _, m := range messages {
			if err := m.Ack(); err != nil {
				s.logError("message ack error: %v", string(m.Data))
			}
		}
		return nil
	}

	if opts.DeadLetterSubject != "" {
		for _, msg := range messages {
			if err := s.conn.Publish(opts.DeadLetterSubject, msg.Data); err != nil {
				s.logError("dead letter publish error: %s", err)
			}
		}
	}

	return err
}

func (s *EventBatchStreamingService) NewStream(name string, opts *StreamingOptions) error {

	if s.isStopping {
		return nil
	}

	if opts == nil {
		opts = &StreamingOptions{}
	}

	if opts.Limit == 0 {
		opts.Limit = 1000
	}
	if opts.AckWait == 0 {
		opts.AckWait = stan.DefaultAckWait
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Second * 5
	}

	var started bool
	var err error
	msgsCh := make(chan *stan.Msg)
	syncCh := make(chan struct{})
	stopCh := make(chan struct{})
	conn := s.Conn()

	if conn == nil {
		return nil
	}

	var msgGroup sync.WaitGroup
	var subscription stan.Subscription

	subscription, err = conn.QueueSubscribe(name, opts.QueueGroup, func(msg *stan.Msg) {

		msgGroup.Add(1)
		defer msgGroup.Done()

		// ждем старта горутины обработки сообщений
		if !started {
			<-syncCh
		}

		s.logDebug("message acquired: %s", string(msg.Data))
		select {
		case msgsCh <- msg:
			s.logDebug("message read: %s", string(msg.Data))
		case <-stopCh:
			// сюда попадаем в случае завершения работы сервиса
			s.logDebug("stop subscription handler, last message was: %s", string(msg.Data))
		case <-time.After(opts.AckWait):
			// просроченные события игнорируем, т.к они будут переотправлены NatsStreaming
			s.logDebug("message skipped: %s", string(msg.Data))
		}
	},
		stan.MaxInflight(opts.Limit),
		stan.SetManualAckMode(),
		stan.AckWait(opts.AckWait),
		stan.DurableName(opts.DurableName),
		stan.DeliverAllAvailable())
	if err != nil {
		return err
	}

	stream := &EventBatchStream{
		subscription: subscription,
		opts:         opts,
		msgsCh:       msgsCh,
		stopCh:       stopCh,
	}

	s.Lock()
	s.streams[name] = stream
	s.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		close(syncCh)
		started = true
		s.processMessages(stopCh, msgsCh, &msgGroup, opts)
		// пришел сигнал завершения работы
		s.logDebug("ready to shutdown, wait subscribe function to close")
		// закроем подписку, чтобы исключить прием новых сообщений
		_ = subscription.Close()
		s.logDebug("subscription closed")
		// если было хотя бы одно сообщение в обработчике, то оно будет обработано перед завершение работы
		close(stopCh)
		// завершение функции обработки сообщения (handler)
		msgGroup.Wait()
		// все сообщения успешно обработаны
	}()

	return nil
}

func (s *EventBatchStreamingService) processMessages(stopCh chan struct{}, msgsCh chan *stan.Msg, msgGroup *sync.WaitGroup, opts *StreamingOptions) {

	timer := time.NewTimer(opts.Timeout)
	defer timer.Stop()

	defer func() {
		s.logDebug("stop main goroutine")
	}()

	for {
		select {
		case <-timer.C:

			var messages []*stan.Msg

		eventLoop:
			for {
				select {
				case msg := <-msgsCh:
					messages = append(messages, msg)
					if len(messages) >= opts.Limit {
						s.logDebug("ready to handle events => by limit")
						break eventLoop
					}
				case <-s.shutdownCh:
					break eventLoop
				case <-time.After(opts.Timeout / 2):
					s.logDebug("ready to handle messages => by timeout")
					break eventLoop
				}
			}

			s.logDebug("messages count: %d", len(messages))
			if len(messages) > 0 {
				_ = s.handleMessages(messages, opts)
			}
			timer.Reset(opts.Timeout)
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *EventBatchStreamingService) Connect(conf NatsStreamingConfig) error {

	if s.isStopping {
		return nil
	}

	conn := s.Conn()
	if conn != nil {
		_ = conn.Close()
	}

	conn, err := stan.Connect(conf.ClusterID, conf.ClientID, stan.NatsURL(conf.URL), stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
		s.AfterConnectionError(conf)
	}))

	if err != nil {
		return err
	}

	s.connLock.Lock()
	s.conn = conn
	s.connLock.Unlock()

	return nil
}

func (s *EventBatchStreamingService) AfterConnectionError(conf NatsStreamingConfig) {

	s.closeLock.Lock()
	defer s.closeLock.Unlock()

	if s.isStopping {
		return
	}

	s.stopProcessing()

	// создадим новое подключение
	var err error
	tick := time.Tick(time.Second * 1)
loop1:
	for {
		if err = s.Connect(conf); err == nil {
			break loop1
		}
		select {
		case <-tick:
		case <-s.stopCh:
			break loop1
		}
	}

	if err != nil {
		s.logError("error on connect: %s", err)
		os.Exit(1)
		return
	}

	streams := s.streams

	// создадим подписки заново
	for name, stream := range streams {
		var err error
		tick := time.Tick(time.Second * 1)
	loop2:
		for {
			if err = s.NewStream(name, stream.opts); err == nil {
				break loop2
			}
			select {
			case <-tick:
			case <-s.stopCh:
				break loop2
			}
		}

		if err != nil {
			s.logError("error on create stream: %s", err)
			os.Exit(1)
		}
	}
}

func (s *EventBatchStreamingService) Conn() stan.Conn {
	s.connLock.RLock()
	defer s.connLock.RUnlock()
	return s.conn
}

func (s *EventBatchStreamingService) Handle(streamName string, handler StreamingHandler) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.handlers[streamName]; ok {
		return errors.New("handler already set")
	}
	s.handlers[streamName] = handler
	return nil
}

func (s *EventBatchStreamingService) Start() error {
	for _, cfg := range s.streamConfig {

		handler, ok := s.handlers[cfg.Name]
		if !ok {
			return fmt.Errorf("no handler for (%s) stream has been set", cfg.Name)
		}

		err := s.NewStream(cfg.Name, &StreamingOptions{
			Limit:        cfg.Limit,
			Timeout:      time.Duration(cfg.Timeout) * time.Second,
			DurableName:  cfg.DurableName,
			QueueGroup:   cfg.QueueGroup,
			EventHandler: handler,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type ErrCloseService struct {
	Errs []error
}

func (e *ErrCloseService) Error() string {
	var errMsg = ""
	for _, err := range e.Errs {
		errMsg += err.Error() + "\n"
	}
	return errMsg
}

func (s *EventBatchStreamingService) stopProcessing() {
	// все обработчики должны завершить свою работу вежливо (graceful)
	// для этого каждый обработчик выполняет следующие действия:
	// 1) processMessages заканчивает обработку текущих сообщений
	// 2) processMessages перед завершением закрывает подписку и ждет завершения функции подписки
	close(s.shutdownCh)
	// ждем завершения всех обработчиков
	s.wg.Wait()
}

func (s *EventBatchStreamingService) Close() error {

	// для уведомления горутины переподключения, если она находится в процессе работы
	// о том, что ее нужно немедленно завершить
	select {
	case s.stopCh <- struct{}{}:
	}

	s.closeLock.Lock()
	defer s.closeLock.Unlock()

	if s.isStopping {
		return nil
	}
	s.isStopping = true

	s.stopProcessing()

	// в этой точке все обработчики завершены и подписки закрыты
	for _, stream := range s.streams {
		if stream.subscription.IsValid() {
			s.logError("subscription not properly closed: %+v", stream.subscription)
		}
		select {
		case <-stream.stopCh:
			s.logDebug("stopped processing in working state")
		default:
			s.logDebug("stopped processing in idle state")
		}
	}

	var errs []error

	if s.conn != nil {
		s.connLock.Lock()
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		s.connLock.Unlock()
	}

	if len(errs) > 0 {
		return &ErrCloseService{
			Errs: errs,
		}
	}

	return nil
}
