package nats.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * JetStream Consumer: 저장된 메시지를 "얼마나/어떻게" 읽어갈지 정의하는 개체.
 *
 * - Pull Consumer: 클라이언트가 능동적으로 "N개 주세요"라고 요청(fetch)한다.
 *   처리 속도를 클라이언트가 직접 제어할 수 있어 워커/배치 처리에 적합하고,
 *   최신 nats.java에서 권장되는 방식이다.
 * - Push Consumer: 서버가 메시지가 생기는 대로 클라이언트에 밀어준다(subscribe).
 *   실시간성이 중요할 때 쓰지만 클라이언트가 처리 속도를 제어하기 어렵다.
 * - 두 방식 모두 "durable" 이름을 주면 컨슈머의 처리 위치(ack 상태)가
 *   서버에 저장되어, 클라이언트가 재시작해도 이어서 읽을 수 있다.
 * - 메시지는 반드시 msg.ack()를 호출해야 "처리 완료"로 서버에 기록된다.
 *   ack하지 않으면 ackWait 시간이 지난 뒤 다시 전달된다(at-least-once).
 */
public class JetStreamConsumerExample {

    private static final String STREAM_NAME = "STREAM_DEMO";
    private static final String SUBJECT = "demo.js.orders";

    public static void main(String[] args) throws IOException, InterruptedException, JetStreamApiException {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            ensureStream(jsm);

            JetStream js = nc.jetStream();

            // 이 예제가 스스로 읽을 데이터를 보장하기 위해 메시지 3개를 새로 발행한다.
            for (int i = 1; i <= 3; i++) {
                js.publish(SUBJECT, ("order-" + i).getBytes(StandardCharsets.UTF_8));
            }

            pullConsumerDemo(js);
            pushConsumerDemo(js);
        }
    }

    /** Pull Consumer: 필요할 때 fetch()로 명시적으로 메시지를 당겨온다. */
    private static void pullConsumerDemo(JetStream js) throws IOException, JetStreamApiException, InterruptedException {
        System.out.println("=== Pull Consumer ===");
        PullSubscribeOptions options = PullSubscribeOptions.builder()
                .durable("pull-durable")
                .build();

        JetStreamSubscription sub = js.subscribe(SUBJECT, options);
        List<Message> messages = sub.fetch(10, Duration.ofSeconds(2));

        for (Message msg : messages) {
            String payload = new String(msg.getData(), StandardCharsets.UTF_8);
            System.out.println("[Pull] received: " + payload + " (seq=" + msg.metaData().streamSequence() + ")");
            msg.ack(); // 처리 완료를 서버에 알림
        }
        sub.unsubscribe();
    }

    /** Push Consumer: 서버가 구독으로 메시지를 밀어주면 동기적으로 nextMessage()로 받는다. */
    private static void pushConsumerDemo(JetStream js) throws IOException, JetStreamApiException, InterruptedException {
        System.out.println("=== Push Consumer ===");
        PushSubscribeOptions options = PushSubscribeOptions.builder()
                .durable("push-durable")
                .build();

        JetStreamSubscription sub = js.subscribe(SUBJECT, options);
        Message msg;
        int count = 0;
        while ((msg = sub.nextMessage(Duration.ofSeconds(1))) != null && count < 3) {
            String payload = new String(msg.getData(), StandardCharsets.UTF_8);
            System.out.println("[Push] received: " + payload + " (seq=" + msg.metaData().streamSequence() + ")");
            msg.ack();
            count++;
        }
        sub.unsubscribe();
    }

    private static void ensureStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        try {
            jsm.getStreamInfo(STREAM_NAME);
        } catch (JetStreamApiException e) {
            StreamConfiguration config = StreamConfiguration.builder()
                    .name(STREAM_NAME)
                    .subjects("demo.js.>")
                    .storageType(StorageType.File)
                    .build();
            jsm.addStream(config);
        }
    }
}
