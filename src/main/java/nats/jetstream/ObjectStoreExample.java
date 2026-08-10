package nats.jetstream;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.ObjectStore;
import io.nats.client.ObjectStoreManagement;
import io.nats.client.api.ObjectInfo;
import io.nats.client.api.ObjectStoreConfiguration;
import io.nats.client.api.StorageType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

/**
 * Object Store: JetStream 위에 만들어진 "대용량 파일/객체 저장소".
 *
 * - KV Store가 작은 값(수백 바이트~수 KB)에 최적화되어 있다면, Object Store는
 *   큰 바이너리(이미지, 파일 등)를 청크 단위로 스트림에 저장하도록 설계되었다.
 * - put()은 이름(name)과 InputStream을 받아 객체를 저장하고,
 *   get()은 이름으로 다시 읽어와 OutputStream에 써준다.
 * - 내부적으로도 결국 JetStream Stream + Consumer 메커니즘을 재사용한다.
 */
public class ObjectStoreExample {

    private static final String BUCKET = "demo-objects";
    private static final String OBJECT_NAME = "greeting.txt";

    public static void main(String[] args)
            throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            ensureBucket(nc);

            ObjectStore os = nc.objectStore(BUCKET);

            // 1) 저장: 바이트 배열을 InputStream으로 감싸서 업로드.
            String content = "Hello, NATS Object Store!";
            byte[] data = content.getBytes(StandardCharsets.UTF_8);
            ObjectInfo info = os.put(OBJECT_NAME, new ByteArrayInputStream(data));
            System.out.println("[Put] stored '" + OBJECT_NAME + "' (" + info.getSize() + " bytes)");

            // 2) 조회: 메타데이터만 먼저 확인 (실제 데이터는 안 옴).
            ObjectInfo meta = os.getInfo(OBJECT_NAME);
            System.out.println("[Info] name=" + meta.getObjectName() + " size=" + meta.getSize()
                    + " digest=" + meta.getDigest());

            // 3) 읽기: OutputStream으로 내용을 받아온다.
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            os.get(OBJECT_NAME, out);
            System.out.println("[Get] content = " + out.toString(StandardCharsets.UTF_8));

            // 4) 삭제
            os.delete(OBJECT_NAME);
            System.out.println("[Delete] '" + OBJECT_NAME + "' deleted");
        }
    }

    private static void ensureBucket(Connection nc) throws IOException, JetStreamApiException {
        ObjectStoreManagement osm = nc.objectStoreManagement();
        try {
            osm.getStatus(BUCKET);
        } catch (JetStreamApiException e) {
            ObjectStoreConfiguration config = ObjectStoreConfiguration.builder()
                    .name(BUCKET)
                    .storageType(StorageType.File)
                    .build();
            osm.create(config);
            System.out.println("[Bucket] created '" + BUCKET + "'");
        }
    }
}
