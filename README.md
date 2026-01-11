# election

Сервис для организации выборов лидера на основе алгоритма Raft.
Сервис позволяет клиентам присоединяться к группам, участвовать в выборах лидера и получать информацию о текущем состоянии группы.

## Основные возможности

- Присоединение к группе: Клиенты могут присоединяться к группам, чтобы участвовать в выборах лидера.
- Выборы лидера: Сервис автоматически выбирает лидера группы на основе алгоритма Raft.
- Callback'и: Клиенты могут зарегистрировать callback'и для получения уведомлений о смене лидера.
- Получение состояния группы: Клиенты могут запрашивать текущее состояние группы, включая информацию о лидере и участниках.

## Использование клиента

```go
// Создание callback'ов
cb := election.LeaderCallbacks{
	OnImLeader: func(event election.LeaderEvent) {
		log.Printf("Текущий клиент стал лидером в группе '%s'. Лидер: %s\n",
			event.Group, event.Leader)
	},
	OnImNotLeader: func(event election.LeaderEvent) {
		log.Printf("Текущий клиент больше не лидер в группе '%s'. Текущий лидер: %s\n",
			event.Group, event.Leader)
	},
	OnLeaderChanged: func(event election.LeaderEvent) {
		log.Printf("В группе '%s' сменился лидер. Новый лидер: %s\n",
			event.Group, event.Leader)
	},
}

// Создание клиента
client, err := election.NewClient("payment", election.WithElectionTimeout(5*time.Second), election.WithCallbacks(cb))
if err != nil {
	log.Error().Msg(err.Error())
}

// Запуск клиента
if err := client.Start(); err != nil {
	log.Printf("Не удалось присоединиться к группе:", err)
}

// Получение состояния группы
state, err := client.GetGroupState("payment")
log.Print(state)

// Закрытие клиента
client.Close()
```

## Опции

При создании нового клиента можно определить ряд опций:
- WithMember: задать собственное имя члена группы
- WithRetryInterval: задать интервал повторного переподключения при разрыве соединения с сервисом
- WithElectionTimeout: задать интервал после которого лидер группы считается устаревшим и проводятся новые выборы (задает первый клиент подключившийся к группе)

Пример:

```go
	client, err := election.NewClient("payment", election.WithElectionTimeout(5*time.Second), election.WithMember("member1"))
```
