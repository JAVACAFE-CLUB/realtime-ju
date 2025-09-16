package com.realtime.collector.application.docs.wikipedia.util;


import com.realtime.collector.application.docs.wikipedia.dto.ShardStats;
import com.realtime.collector.application.docs.wikipedia.dto.WikiPage;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;


/**
 * Wikipedia 파싱 상태와 샤딩/업로드를 관리하는 컨텍스트입니다.
 * <p>
 * - 요소 스택을 유지해 부모/현재 요소를 추적하고, - 텍스트 버퍼링을 통해 필요한 값(title, text 등)을 누적하며, - 페이지마다 NDJSON 레코드를 생성해 샤드 파일로 쓰고 업로드합니다.
 * </p>
 */
@Getter
@Builder
public class WikiParsingContext {
    private static final Set<String> TEXT_BUFFER_ELEMENTS = Set.of(
            "title", "text", "username", "ns", "id", "timestamp"
    );

    private final String lang;
    private final String dumpDate;
    private final String collectionId;
    private final int pagesPerShard;
    private final String basePrefix;
    private final WikiShardManager shardManager;

    private final Deque<String> elementStack = new ArrayDeque<>();
    private StringBuilder textBuffer;
    private boolean isBufferingText = false;

    private WikiShardManager.ShardWriter currentShard;
    private int shardIndex = 0;
    private int totalPages = 0;
    private int totalShards = 0;
    private long totalBytes = 0L;
    private final List<String> shardKeys = new ArrayList<>();


    /**
     * 현재 요소를 스택에 push 합니다.
     */
    public void pushElement(String elementName) {
        elementStack.push(elementName);
    }

    /**
     * 현재 요소를 스택에서 pop 합니다.
     */
    public void popElement() {
        if (!elementStack.isEmpty()) {
            elementStack.pop();
        }
    }

    /**
     * 부모 요소 이름을 반환합니다. 없으면 null.
     */
    public String getParentElement() {
        return elementStack.size() >= 2 ?
                elementStack.stream().skip(1).findFirst().orElse(null) : null;
    }

    /**
     * 해당 요소의 텍스트를 버퍼링해야 하는지 여부를 반환합니다.
     */
    public boolean shouldBufferText(String elementName) {
        return TEXT_BUFFER_ELEMENTS.contains(elementName);
    }

    /**
     * 텍스트 버퍼링을 시작합니다.
     */
    public void startTextBuffering() {
        textBuffer = new StringBuilder(1024);
        isBufferingText = true;
    }

    /**
     * 버퍼에 텍스트를 추가합니다.
     */
    public void appendText(String text) {
        if (textBuffer != null) {
            textBuffer.append(text);
        }
    }

    /**
     * 누적된 텍스트를 문자열로 반환합니다.
     */
    public String getBufferedText() {
        return textBuffer != null ? textBuffer.toString() : "";
    }

    /**
     * 텍스트 버퍼링을 중단하고 버퍼를 초기화합니다.
     */
    public void stopTextBuffering() {
        textBuffer = null;
        isBufferingText = false;
    }

    /**
     * 페이지를 현재 샤드에 기록하고 통계를 갱신합니다.
     */
    public void addPage(WikiPage page) throws IOException {
        ensureShardAvailable();
        writePageToShard(page);
        currentShard.incrementPages();
        totalPages++;
    }

    /**
     * 샤드가 가득 찼다면 업로드 후 새 샤드를 생성합니다.
     */
    private void ensureShardAvailable() throws IOException {
        if (currentShard == null || currentShard.getPagesInShard() >= pagesPerShard) {
            if (currentShard != null) {
                uploadCurrentShard();
            }
            currentShard = shardManager.createShard(shardIndex++);
        }
    }

    /**
     * 페이지의 필드를 NDJSON 한 레코드로 직렬화하여 샤드 파일에 씁니다.
     */
    private void writePageToShard(WikiPage page) throws IOException {
        try {
            var gen = currentShard.generator();
            gen.writeStartObject();
            gen.writeStringField("collectionId", collectionId);
            gen.writeStringField("lang", lang);
            gen.writeStringField("dumpDate", dumpDate);
            gen.writeStringField("pageId", nullSafe(page.getPageId()));
            gen.writeStringField("title", nullSafe(page.getTitle()));
            gen.writeNumberField("ns", page.getNs() != null ? page.getNs() : -1);
            gen.writeStringField("redirectTitle", nullSafe(page.getRedirectTitle()));
            gen.writeStringField("revisionId", nullSafe(page.getRevisionId()));
            gen.writeStringField("timestamp", nullSafe(page.getTimestamp()));
            gen.writeStringField("contributor", nullSafe(page.getContributor()));
            gen.writeStringField("text", nullSafe(page.getText()));
            gen.writeEndObject();
            gen.writeRaw('\n');
        } catch (Exception e) {
            throw new IOException("페이지 데이터 쓰기 실패", e);
        }
    }

    private String nullSafe(String value) {
        return value != null ? value : "";
    }

    /**
     * 현재 샤드를 업로드하고 통계를 갱신합니다.
     */
    private void uploadCurrentShard() {
        try {
            currentShard.close();

            String shardKey = String.format("%s/part-%05d.ndjson.gz", basePrefix, shardIndex - 1);
            Map<String, String> metadata = Map.of(
                    "collection-id", collectionId,
                    "lang", lang,
                    "dump-date", dumpDate,
                    "pages", String.valueOf(currentShard.getPagesInShard())
            );

            long uploadedBytes = shardManager.uploadShard(currentShard, shardKey, metadata);

            shardKeys.add(shardKey);
            totalBytes += uploadedBytes;
            totalShards++;

        } catch (Exception e) {
            throw new RuntimeException("샤드 업로드 실패", e);
        }
    }

    /**
     * 파싱 종료 시 열려있는 샤드가 있으면 업로드합니다.
     */
    public void finalizeParsing() {
        if (currentShard != null) {
            uploadCurrentShard();
            currentShard = null;
        }
    }

    /**
     * 수집 통계를 반환합니다.
     */
    public ShardStats getStats() {
        return ShardStats.builder()
                .lang(lang)
                .dumpDate(dumpDate)
                .pagesTotal(totalPages)
                .shardsTotal(totalShards)
                .bytesTotal(totalBytes)
                .shardKeys(shardKeys)
                .build();
    }
}


