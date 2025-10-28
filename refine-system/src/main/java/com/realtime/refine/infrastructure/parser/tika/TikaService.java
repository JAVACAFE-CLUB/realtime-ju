package com.realtime.refine.infrastructure.parser.tika;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TikaService {

    @Value("${tika.max-string-length:100000000}")
    private int maxStringLength;

    @Value("${tika.timeout:60000}")
    private long timeoutMs;

    public TikaResult parse(InputStream inputStream) {
        long start = System.currentTimeMillis();
        try {
            log.debug("🔄 Tika 파싱 시작 - maxStringLength={}, timeout={}ms", maxStringLength, timeoutMs);
            
            // [1단계] Tika 핵심 클래스 리플렉션 로딩(컴파일 의존성 최소화)
            Class<?> bodyHandlerClz = Class.forName("org.apache.tika.sax.BodyContentHandler");
            Class<?> metadataClz = Class.forName("org.apache.tika.metadata.Metadata");
            Class<?> parserClz = Class.forName("org.apache.tika.parser.AutoDetectParser");
            Class<?> parseContextClz = Class.forName("org.apache.tika.parser.ParseContext");

            Constructor<?> handlerCtor = bodyHandlerClz.getConstructor(int.class);
            Object handler = handlerCtor.newInstance(maxStringLength);
            Object metadata = metadataClz.getConstructor().newInstance();
            Object parser = parserClz.getConstructor().newInstance();
            Object ctx = parseContextClz.getConstructor().newInstance();

            // [2단계] parse 메서드 참조 획득
            Method parseMethod = parserClz.getMethod(
                    "parse",
                    java.io.InputStream.class,
                    Class.forName("org.xml.sax.ContentHandler"),
                    metadataClz,
                    parseContextClz
            );

            // [3단계] 타임아웃을 위한 별도 스레드 실행
            log.debug("🔄 Tika 파싱 실행 중...");
            Callable<Void> task = () -> {
                try {
                    parseMethod.invoke(parser, inputStream, handler, metadata, ctx);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return null;
            };

            var executor = Executors.newSingleThreadExecutor(new DaemonFactory());
            try {
                Future<Void> future = executor.submit(task);
                future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } finally {
                executor.shutdownNow();
            }

            // [4단계] 메타데이터 추출
            Map<String, String> map = new HashMap<>();
            Method namesMethod = metadataClz.getMethod("names");
            String[] names = (String[]) namesMethod.invoke(metadata);
            Method getMethod = metadataClz.getMethod("get", String.class);
            for (String name : names) {
                String v = (String) getMethod.invoke(metadata, name);
                map.put(name, v);
            }

            // [5단계] 결과 객체 구성
            String content = handler.toString();
            String ct = (String) getMethod.invoke(metadata, "Content-Type");
            String cs = (String) getMethod.invoke(metadata, "Content-Encoding");
            String lang = (String) getMethod.invoke(metadata, "language");

            long elapsed = System.currentTimeMillis() - start;
            log.debug("✅ Tika 파싱 완료 - elapsed={}ms, contentLength={}, contentType={}, language={}", 
                    elapsed, content != null ? content.length() : 0, ct, lang);

            return TikaResult.builder()
                    .content(content)
                    .metadata(map)
                    .contentType(ct)
                    .charset(cs)
                    .language(lang)
                    .build();
        } catch (Exception e) {
            // [에러 처리] 원인 포함하여 래핑
            long elapsed = System.currentTimeMillis() - start;
            log.error("❌ Tika 파싱 실패 - elapsed={}ms, error={}", elapsed, e.getMessage(), e);
            throw new RuntimeException("Tika parse failed", e);
        }
    }

    static class DaemonFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "tika-timeout");
            t.setDaemon(true);
            return t;
        }
    }
}



