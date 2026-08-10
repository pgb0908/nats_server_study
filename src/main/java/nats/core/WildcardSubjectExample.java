package nats.core;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;

/**
 * Subject는 점(.)으로 구분된 계층 구조를 가지며, 두 가지 와일드카드를 지원한다.
 *
 * - '*' : 정확히 한 토큰(레벨)에 매치. 예) "sensor.*.temp" -> "sensor.room1.temp"는 매치,
 *         "sensor.room1.hall.temp"는 매치 안 됨.
 * - '>' : 그 위치부터 나머지 전체 토큰에 매치 (반드시 subject의 마지막에만 올 수 있음).
 *         예) "sensor.>" -> "sensor.room1.temp", "sensor.room1.hall.temp" 모두 매치.
 *
 * 이 패턴은 모니터링, 라우팅, 멀티테넌시 subject 설계에 자주 쓰인다.
 */
public class WildcardSubjectExample {

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // '*' 구독: 정확히 "sensor.<한 토큰>.temp" 형태만 받는다.
            Dispatcher starDispatcher = nc.createDispatcher(msg ->
                    System.out.println("[* subscriber]  subject='" + msg.getSubject() + "' matched 'sensor.*.temp'"));
            starDispatcher.subscribe("sensor.*.temp");

            // '>' 구독: "sensor." 이하 모든 하위 subject를 다 받는다.
            Dispatcher gtDispatcher = nc.createDispatcher(msg ->
                    System.out.println("[> subscriber]  subject='" + msg.getSubject() + "' matched 'sensor.>'"));
            gtDispatcher.subscribe("sensor.>");

            Thread.sleep(200);

            publish(nc, "sensor.room1.temp");        // * 매치, > 매치
            publish(nc, "sensor.room1.hall.temp");    // * 불매치, > 매치
            publish(nc, "sensor.room2.temp");         // * 매치, > 매치
            publish(nc, "other.topic");                // 둘 다 불매치

            Thread.sleep(500);
        }
    }

    private static void publish(Connection nc, String subject) {
        System.out.println("[Publisher] publishing to '" + subject + "'");
        nc.publish(subject, "reading".getBytes(StandardCharsets.UTF_8));
    }
}
