package nats.core;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;

/**
 * NATS의 가장 기본적인 통신 패턴: Publish/Subscribe.
 *
 * - Publisher는 "subject"라는 이름의 채널에 메시지를 던진다.
 * - Subscriber는 같은 subject를 구독하고 있으면 메시지를 받는다.
 * - Core NATS의 pub/sub은 "at-most-once"다: 메시지를 받을 때 구독자가 없으면
 *   그 메시지는 그냥 유실된다 (영속성 없음). 영속성이 필요하면 JetStream을 쓴다.
 * - 서버 주소 기본값은 nats://localhost:4222 (docker-compose.yml 참고).
 */
public class PublishSubscribeExample {

    private static final String SUBJECT = "demo.greeting";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // 1) 구독 등록: 비동기 Dispatcher는 콜백으로 메시지를 처리한다.
            Dispatcher dispatcher = nc.createDispatcher(msg -> {
                String payload = new String(msg.getData(), StandardCharsets.UTF_8);
                System.out.println("[Subscriber] received on '" + msg.getSubject() + "': " + payload);
            });
            dispatcher.subscribe(SUBJECT);

            // 구독이 서버에 등록될 시간을 잠깐 확보 (학습용 데모라 단순 sleep 사용)
            Thread.sleep(200);

            // 2) 발행: publish는 fire-and-forget이며 응답을 기다리지 않는다.
            for (int i = 1; i <= 3; i++) {
                String message = "hello #" + i;
                nc.publish(SUBJECT, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("[Publisher] sent: " + message);
            }

            // 메시지가 비동기로 도착할 시간을 확보 후 종료
            Thread.sleep(500);
        }
    }
}
