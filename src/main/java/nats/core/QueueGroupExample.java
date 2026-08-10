package nats.core;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Queue Group: 여러 구독자가 같은 큐 그룹 이름으로 구독하면,
 * 각 메시지는 그 그룹 내 "단 하나의" 구독자에게만 전달된다 (로드밸런싱).
 *
 * - 일반 pub/sub: 구독자가 3명이면 메시지 1개가 3명 모두에게 전달됨 (broadcast).
 * - 큐 그룹 pub/sub: 구독자 3명이 같은 큐 이름을 쓰면 메시지 1개는 그중 1명에게만 감.
 * - 워커 풀(worker pool) 패턴을 구현할 때 표준적으로 쓰이는 방식이다.
 */
public class QueueGroupExample {

    private static final String SUBJECT = "demo.work";
    private static final String QUEUE_GROUP = "workers";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            AtomicInteger[] counters = new AtomicInteger[3];

            // 1) 같은 큐 그룹으로 워커 3개를 등록.
            for (int i = 0; i < 3; i++) {
                final int workerId = i + 1;
                counters[i] = new AtomicInteger(0);
                Dispatcher dispatcher = nc.createDispatcher(msg -> {
                    String job = new String(msg.getData(), StandardCharsets.UTF_8);
                    counters[workerId - 1].incrementAndGet();
                    System.out.println("[Worker " + workerId + "] processed: " + job);
                });
                // subscribe(subject, queueGroup) — 세 번째 인자가 큐 그룹 이름.
                dispatcher.subscribe(SUBJECT, QUEUE_GROUP);
            }
            Thread.sleep(200);

            // 2) 작업 9개를 발행 -> 큐 그룹 덕분에 3개의 워커가 나눠서 처리한다.
            for (int i = 1; i <= 9; i++) {
                nc.publish(SUBJECT, ("job-" + i).getBytes(StandardCharsets.UTF_8));
            }

            Thread.sleep(500);

            System.out.println("--- 분배 결과 ---");
            for (int i = 0; i < counters.length; i++) {
                System.out.println("Worker " + (i + 1) + " handled " + counters[i].get() + " jobs");
            }
        }
    }
}
