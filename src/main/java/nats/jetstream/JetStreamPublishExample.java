package nats.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * JetStream: Core NATS 위에 얹는 영속성 레이어.
 *
 * - Core NATS의 publish는 구독자가 없으면 메시지가 사라진다.
 * - JetStream은 "Stream"이라는 저장소를 만들고, 특정 subject 패턴에 매치되는
 *   메시지를 디스크(또는 메모리)에 저장한다. 구독자가 나중에 붙어도 저장된
 *   메시지를 다시 읽을 수 있다 (at-least-once 전달).
 * - Stream은 subject 패턴, 보관 정책(retention), 저장 방식(storage) 등을 가진다.
 *
 * 이 예제는 STREAM_DEMO 라는 스트림을 만들고 demo.js.> 패턴의 메시지를 저장한다.
 */
public class JetStreamPublishExample {

    private static final String STREAM_NAME = "STREAM_DEMO";
    private static final String SUBJECT = "demo.js.orders";

    public static void main(String[] args) throws IOException, InterruptedException, JetStreamApiException {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            createStreamIfAbsent(jsm);

            JetStream js = nc.jetStream();

            // js.publish는 nc.publish와 달리 서버가 실제로 저장했다는 확인(PublishAck)을 반환한다.
            for (int i = 1; i <= 3; i++) {
                String payload = "order-" + i;
                PublishAck ack = js.publish(SUBJECT, payload.getBytes(StandardCharsets.UTF_8));
                System.out.println("[Publish] '" + payload + "' -> stream=" + ack.getStream()
                        + " seq=" + ack.getSeqno());
            }

            StreamInfo info = jsm.getStreamInfo(STREAM_NAME);
            System.out.println("[Stream] " + STREAM_NAME + " now holds "
                    + info.getStreamState().getMsgCount() + " messages");
        }
    }

    private static void createStreamIfAbsent(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        try {
            jsm.getStreamInfo(STREAM_NAME);
            System.out.println("[Stream] '" + STREAM_NAME + "' already exists, reusing it");
        } catch (JetStreamApiException e) {
            StreamConfiguration config = StreamConfiguration.builder()
                    .name(STREAM_NAME)
                    .subjects("demo.js.>")
                    .storageType(StorageType.File)
                    .build();
            jsm.addStream(config);
            System.out.println("[Stream] created '" + STREAM_NAME + "' for subjects 'demo.js.>'");
        }
    }
}
