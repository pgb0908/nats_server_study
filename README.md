# NATS 학습 프로젝트

NATS(server)의 기초 개념을 Core NATS부터 JetStream까지 실습으로 익히기 위한 프로젝트.

## 기본 개념

- **Subject**: 메시지를 주고받는 채널 이름. `sensor.room1.temp` 처럼 점(`.`)으로 계층을 표현한다.
- **Publish/Subscribe**: publisher가 subject에 메시지를 던지면 그 subject를 구독 중인 모든 subscriber가 받는다.
- **Core NATS**: 기본 전송 계층. 메시지를 저장하지 않는 at-most-once 전달 — 구독자가 없으면 메시지는 유실된다.
- **JetStream**: Core NATS 위에 얹는 영속성 레이어. 메시지를 스트림에 저장해 at-least-once 전달, 재생(replay), Consumer 상태 추적 등을 지원한다. (다음 단계에서 다룸)

## 1. NATS 서버 띄우기

```bash
docker compose up -d
```

- 클라이언트 포트: `4222`
- 모니터링 UI: http://localhost:8222
- `-js` 플래그로 JetStream이 활성화되어 있고, 데이터는 `./data`(호스트 볼륨)에 저장된다.

종료:

```bash
docker compose down
```

## 2. 예제 실행

각 예제는 `nats.core` 패키지 안에 독립적인 `main()`을 가진 클래스로 되어 있다. Gradle로 실행:

```bash
./gradlew run -PmainClass=nats.core.PublishSubscribeExample
./gradlew run -PmainClass=nats.core.RequestReplyExample
./gradlew run -PmainClass=nats.core.QueueGroupExample
./gradlew run -PmainClass=nats.core.WildcardSubjectExample
```

### Core NATS 예제 정리

| 클래스 | 개념 | 핵심 포인트 |
|---|---|---|
| `PublishSubscribeExample` | 기본 Pub/Sub | 구독자가 없으면 메시지는 그냥 사라진다 (영속성 없음) |
| `RequestReplyExample` | Request/Reply | 임시 reply subject를 이용한 동기식 RPC. 응답자가 없으면 타임아웃 |
| `QueueGroupExample` | Queue Group | 같은 큐 그룹의 구독자들이 메시지를 나눠 받는다 (로드밸런싱) |
| `WildcardSubjectExample` | Wildcard Subject | `*`는 토큰 1개, `>`는 나머지 전체 토큰에 매치 |

## 3. JetStream 예제 실행

`nats.jetstream` 패키지에 있으며, 실행 전 `docker compose up -d`로 서버가 떠 있어야 한다 (JetStream은 `-js` 플래그로 이미 활성화되어 있음).

```bash
./gradlew run -PmainClass=nats.jetstream.JetStreamPublishExample
./gradlew run -PmainClass=nats.jetstream.JetStreamConsumerExample
./gradlew run -PmainClass=nats.jetstream.KeyValueStoreExample
./gradlew run -PmainClass=nats.jetstream.ObjectStoreExample
```

### JetStream 개념

- **Stream**: subject 패턴에 매치되는 메시지를 저장하는 저장소. Core NATS와 달리 구독자가 없어도 메시지가 남는다.
- **Consumer**: Stream에 저장된 메시지를 어떻게(순서/속도/재시도) 읽어갈지 정의하는 개체.
  - **Pull**: 클라이언트가 `fetch()`로 능동적으로 당겨온다. 처리 속도 제어가 쉬워 최신 nats.java에서 권장.
  - **Push**: 서버가 메시지를 밀어준다. 실시간성은 좋지만 배압(backpressure) 제어가 어렵다.
  - **Durable**: 이름을 지정하면 ack 상태(어디까지 읽었는지)가 서버에 저장되어 재시작 후에도 이어 읽을 수 있다.
- **Ack**: consumer가 메시지를 받아도 `msg.ack()`를 호출해야 "처리 완료"로 기록된다. 안 하면 `ackWait` 이후 재전달된다 (at-least-once).
- **KV Store**: JetStream 위에 만든 키-값 저장소. 내부적으로 key마다 subject를 가진 전용 Stream이다. `put`/`get`/`history`/`delete` 지원.
- **Object Store**: JetStream 위에 만든 대용량 객체(파일) 저장소. 청크 단위로 Stream에 저장한다.

### JetStream 예제 정리

| 클래스 | 개념 | 핵심 포인트 |
|---|---|---|
| `JetStreamPublishExample` | Stream 생성 + Publish | `js.publish()`는 서버 저장 확인(`PublishAck`)을 반환 |
| `JetStreamConsumerExample` | Pull/Push Consumer | 같은 메시지를 Pull(`fetch`)과 Push(`nextMessage`) 두 방식으로 각각 소비 |
| `KeyValueStoreExample` | KV Store | put/get/history/delete로 리비전 기반 키-값 저장 확인 |
| `ObjectStoreExample` | Object Store | 바이트 데이터를 객체로 put/get/delete |

각 예제는 실행할 때마다 필요한 Stream/Bucket이 없으면 자동으로 생성한다 (이미 있으면 재사용).
