package nats.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.Nats;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.StorageType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * KV Store: JetStream 위에 만들어진 "분산 키-값 저장소".
 *
 * - 내부적으로는 각 key가 subject(예: demo-kv.<key>)가 되는 전용 Stream이다.
 *   즉 KV도 결국 JetStream의 메시지 저장/재생 메커니즘을 재사용한다.
 * - put()은 새 값을 쓰고, get()은 최신 값을 읽는다.
 * - 같은 key에 여러 번 put하면 history(리비전)가 쌓이고, history()로 과거 값도
 *   조회할 수 있다 (기본적으로 revision 개수 제한이 있음).
 * - Redis 같은 외부 캐시 없이 "가볍게 상태를 공유"하고 싶을 때 적합하다.
 */
public class KeyValueStoreExample {

    private static final String BUCKET = "demo-kv";

    public static void main(String[] args) throws IOException, JetStreamApiException, InterruptedException {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            ensureBucket(nc);

            KeyValue kv = nc.keyValue(BUCKET);

            // 1) 쓰기: put()은 새 리비전 번호를 반환한다.
            long rev1 = kv.put("user.count", "10".getBytes(StandardCharsets.UTF_8));
            System.out.println("[Put] user.count = 10 (revision " + rev1 + ")");

            long rev2 = kv.put("user.count", "11".getBytes(StandardCharsets.UTF_8));
            System.out.println("[Put] user.count = 11 (revision " + rev2 + ")");

            // 2) 읽기: 항상 최신 값을 가져온다.
            KeyValueEntry entry = kv.get("user.count");
            System.out.println("[Get] user.count = " + entry.getValueAsString()
                    + " (revision " + entry.getRevision() + ")");

            // 3) 히스토리: 해당 key에 대해 저장된 과거 리비전들을 확인.
            List<KeyValueEntry> history = kv.history("user.count");
            System.out.println("[History] user.count has " + history.size() + " revision(s):");
            for (KeyValueEntry h : history) {
                System.out.println("  rev=" + h.getRevision() + " value=" + h.getValueAsString());
            }

            // 4) 삭제: 값을 지우면 tombstone 리비전이 하나 추가된다.
            kv.delete("user.count");
            System.out.println("[Delete] user.count deleted");
        }
    }

    private static void ensureBucket(Connection nc) throws IOException, JetStreamApiException {
        KeyValueManagement kvm = nc.keyValueManagement();
        try {
            kvm.getStatus(BUCKET);
        } catch (JetStreamApiException e) {
            KeyValueConfiguration config = KeyValueConfiguration.builder()
                    .name(BUCKET)
                    .storageType(StorageType.File)
                    .maxHistoryPerKey(5)
                    .build();
            kvm.create(config);
            System.out.println("[Bucket] created '" + BUCKET + "'");
        }
    }
}
