package nats.core;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Request/Reply 패턴: 동기식 RPC처럼 응답을 기다리는 통신.
 *
 * - 내부적으로는 두 번의 pub/sub이다: 요청자가 임시로 생성한 "reply subject"를
 *   요청 메시지에 실어 보내면, 응답자는 그 subject로 응답을 publish한다.
 * - 요청자는 request()를 호출하면 내부적으로 reply subject를 구독하고
 *   타임아웃 동안 응답을 기다린다.
 * - 응답자(서비스)가 떠 있지 않으면 요청은 타임아웃으로 실패한다 (Core NATS는
 *   메시지를 큐잉하지 않기 때문).
 */
public class RequestReplyExample {

    private static final String SUBJECT = "demo.echo";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // 1) 응답자(responder) 역할: 요청을 받아 대문자로 바꿔 되돌려준다.
            Dispatcher dispatcher = nc.createDispatcher(msg -> {
                String request = new String(msg.getData(), StandardCharsets.UTF_8);
                String reply = request.toUpperCase();
                System.out.println("[Responder] got '" + request + "' -> replying '" + reply + "'");
                nc.publish(msg.getReplyTo(), reply.getBytes(StandardCharsets.UTF_8));
            });
            dispatcher.subscribe(SUBJECT);
            Thread.sleep(200);

            // 2) 요청자(requester) 역할: 응답을 최대 1초 기다린다.
            Message response = nc.request(
                    SUBJECT,
                    "hello nats".getBytes(StandardCharsets.UTF_8),
                    Duration.ofSeconds(1)
            );

            if (response == null) {
                System.out.println("[Requester] timed out waiting for a reply");
            } else {
                String body = new String(response.getData(), StandardCharsets.UTF_8);
                System.out.println("[Requester] got reply: " + body);
            }
        }
    }
}
