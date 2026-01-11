# Настройка TLS для HTTP/3 сервера

## Обзор

Данный документ описывает рекомендуемые настройки TLS для HTTP/3 сервера с поддержкой взаимной аутентификации клиентов.

## Настройки ClientAuth

### Доступные варианты ClientAuth:

1. **`tls.NoClientCert`** - Клиентские сертификаты не требуются
2. **`tls.RequestClientCert`** - Запрашивает клиентский сертификат, но не проверяет его
3. **`tls.RequireAnyClientCert`** - Требует клиентский сертификат, но не проверяет его
4. **`tls.VerifyClientCertIfGiven`** - Проверяет клиентский сертификат, если он предоставлен
5. **`tls.RequireAndVerifyClientCert`** - Требует и проверяет клиентский сертификат ✅ **Рекомендуется**

### Рекомендуемая настройка:

```go
ClientAuth: tls.RequireAndVerifyClientCert
```

Эта настройка обеспечивает максимальную безопасность, требуя от всех клиентов предоставления действительного сертификата, подписанного доверенным CA.

## Полная конфигурация TLS для HTTP/3

```go
tlsConf := &tls.Config{
    NextProtos:   []string{"h3"},                    // HTTP/3
    Certificates: []tls.Certificate{tlsCert},        // Серверный сертификат
    ClientCAs:    caPool,                           // CA
    ClientAuth:   tls.RequireAndVerifyClientCert,   //  client certificate should be requested during the handshake, and that at least one valid certificate is required to be sent by the client
    MinVersion:   tls.VersionTLS13,                 // TLS 1.3 минимум
}
```

## Переменные окружения

### Серверные сертификаты:
- `TLS_CERT` - PEM-encoded серверный сертификат
- `TLS_KEY` - PEM-encoded приватный ключ сервера
- `CA_CERT` - PEM-encoded CA сертификат для проверки клиентов

### Клиентские сертификаты:
- `CLIENT_TLS_CERT` - PEM-encoded клиентский сертификат
- `CLIENT_TLS_KEY` - PEM-encoded приватный ключ клиента

## Особенности HTTP/3

### 1. Обязательное использование TLS 1.3
HTTP/3 требует TLS 1.3 или выше. Более старые версии TLS не поддерживаются.

### 2. ALPN (Application-Layer Protocol Negotiation)
Обязательно указывать `NextProtos: []string{"h3"}` для корректного согласования протокола.

### 3. Cipher Suites
Для TLS 1.3 используются только AEAD cipher suites:
- `TLS_AES_128_GCM_SHA256`
- `TLS_AES_256_GCM_SHA384`
- `TLS_CHACHA20_POLY1305_SHA256`

### 4. Elliptic Curves
Рекомендуемые кривые для производительности и безопасности:
- `X25519` - быстрая и безопасная
- `P-256` - широко поддерживается
- `P-384` - для повышенной безопасности

## Настройка клиента

Клиенты должны быть настроены с соответствующими сертификатами:

```go
// Использование новой функции для создания TLS конфигурации клиента
tlsConfig, err := cert.GetTLSConfigForClient(false) // false = проверять сертификат сервера
if err != nil {
    return nil, fmt.Errorf("failed to create TLS config: %w", err)
}

client := &http3.Client{
    TLSClientConfig: tlsConfig,
}
```

## Безопасность

### Рекомендации:
1. **Всегда используйте TLS 1.3** для HTTP/3
2. **Отключите renegotiation** (`tls.RenegotiateNever`)
3. **Используйте строгую проверку сертификатов** (`RequireAndVerifyClientCert`)
4. **Регулярно обновляйте сертификаты**
5. **Используйте сильные cipher suites**
6. **Настройте правильные curve preferences**

### Мониторинг:
- Отслеживайте ошибки TLS handshake
- Мониторьте истечение сертификатов
- Логируйте попытки подключения с недействительными сертификатами

## Производительность

### Session Tickets
Включение session tickets (`SessionTicketsDisabled: false`) улучшает производительность за счет сокращения времени handshake при повторных подключениях.

### Connection Pooling
HTTP/3 поддерживает мультиплексирование, что позволяет использовать одно соединение для множественных запросов.

## Отладка

### Логирование TLS событий:
```go
tlsConf.KeyLogWriter = os.Stdout // Только для отладки!
```

**Внимание:** Никогда не используйте `KeyLogWriter` в production!

### Проверка сертификатов:
```bash
# Проверка серверного сертификата
openssl x509 -in server.crt -text -noout

# Проверка клиентского сертификата
openssl x509 -in client.crt -text -noout

# Проверка CA сер��ификата
openssl x509 -in ca.crt -text -noout
```

## Примеры использования

### Создание самоподписанных сертификатов для тестирования:
```bash
# CA ключ и сертификат
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -sha256 -subj "/C=RU/ST=Moscow/O=Test/CN=Test CA" -days 3650 -out ca.crt

# Серверный ключ и сертификат
openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr -subj "/C=RU/ST=Moscow/O=Test/CN=localhost"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256

# Клиентский ключ и сертификат
openssl genrsa -out client.key 4096
openssl req -new -key client.key -out client.csr -subj "/C=RU/ST=Moscow/O=Test/CN=client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365 -sha256
```

### Установка переменных окружения:
```bash
export TLS_CERT="$(cat server.crt)"
export TLS_KEY="$(cat server.key)"
export CA_CERT="$(cat ca.crt)"
export CLIENT_TLS_CERT="$(cat client.crt)"
export CLIENT_TLS_KEY="$(cat client.key)"
```
