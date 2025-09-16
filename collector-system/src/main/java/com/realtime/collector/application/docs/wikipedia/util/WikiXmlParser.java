package com.realtime.collector.application.docs.wikipedia.util;

import com.realtime.collector.application.docs.wikipedia.dto.WikiPage;
import java.io.IOException;
import java.util.Optional;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.springframework.stereotype.Component;

/**
 * Wikipedia 덤프 XML을 스트리밍으로 파싱하여 {@link WikiParsingContext}에 페이지를 전달하는 파서입니다.
 * <p>
 * StAX(XMLStreamReader)를 사용해 메모리 사용량을 최소화하며, 페이지 단위로 필요한 필드만 추출합니다. 페이지 경계(page start/end)에서
 * {@link WikiParsingContext#addPage(com.realtime.collector.application.docs.wikipedia.dto.WikiPage)} 를 호출하여 샤딩 및 업로드
 * 파이프라인으로 전송합니다.
 * </p>
 */
@Component
public class WikiXmlParser {

    /**
     * XML 스트림을 순회하며 Wikipedia 페이지를 파싱합니다.
     *
     * @param reader  XML 스트림 리더 (StAX)
     * @param context 파싱 컨텍스트(샤딩/통계/상태 관리)
     * @throws XMLStreamException XML 읽기 예외
     * @throws IOException        샤드 쓰기 등 I/O 예외
     */
    public void parse(XMLStreamReader reader, WikiParsingContext context) throws XMLStreamException, IOException {
        WikiPage.WikiPageBuilder pageBuilder = null;

        while (reader.hasNext()) {
            int event = reader.next();

            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> pageBuilder = handleStartElement(reader, context, pageBuilder);
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> handleTextContent(reader, context);
                case XMLStreamConstants.END_ELEMENT -> pageBuilder = handleEndElement(reader, context, pageBuilder);
                default -> {
                }
            }
        }

        context.finalizeParsing();
    }

    /**
     * START_ELEMENT 처리: 페이지 시작 시 빌더를 생성하고, 필요한 경우 텍스트 버퍼링을 시작합니다.
     */
    private WikiPage.WikiPageBuilder handleStartElement(XMLStreamReader reader, WikiParsingContext context,
                                                        WikiPage.WikiPageBuilder builder) {
        context.pushElement(reader.getLocalName());

        if ("page".equals(reader.getLocalName())) {
            builder = WikiPage.builder();
        } else if ("redirect".equals(reader.getLocalName()) && builder != null) {
            Optional.ofNullable(WikiXmlUtil.getAttribute(reader, "title"))
                    .ifPresent(builder::redirectTitle);
        }

        // 텍스트를 버퍼링할 요소들
        if (builder != null && context.shouldBufferText(reader.getLocalName())) {
            context.startTextBuffering();
        }

        return builder;
    }

    /**
     * CHARACTERS/CDATA 처리: 버퍼링 중인 경우 텍스트를 누적합니다.
     */
    private void handleTextContent(XMLStreamReader reader, WikiParsingContext context) {
        if (context.isBufferingText() && !reader.isWhiteSpace()) {
            context.appendText(reader.getText());
        }
    }

    /**
     * END_ELEMENT 처리: 버퍼를 읽어 필드를 설정하고, 페이지 종료 시 컨텍스트로 전달합니다.
     */
    private WikiPage.WikiPageBuilder handleEndElement(XMLStreamReader reader, WikiParsingContext context,
                                                      WikiPage.WikiPageBuilder builder)
            throws IOException {
        // 텍스트 버퍼에서 값 추출 및 설정
        if (builder != null && context.isBufferingText()) {
            setPageField(builder, reader.getLocalName(), context.getBufferedText(), context.getParentElement());
        }

        context.stopTextBuffering();

        // 페이지 완료 처리
        if ("page".equals(reader.getLocalName()) && builder != null) {
            context.addPage(builder.build());
            builder = null;
        }

        context.popElement();
        return builder;
    }

    /**
     * 요소 이름과 상위 요소를 기반으로 {@link com.realtime.collector.application.docs.wikipedia.dto.WikiPage} 빌더에 값을 설정합니다.
     */
    private void setPageField(WikiPage.WikiPageBuilder builder, String elementName, String value,
                              String parentElement) {
        switch (elementName) {
            case "title" -> builder.title(value);
            case "ns" -> builder.ns(WikiXmlUtil.parseIntSafe(value));
            case "id" -> {
                if ("revision".equals(parentElement)) {
                    builder.revisionId(value);
                } else if (!"contributor".equals(parentElement)) {
                    builder.pageId(value);
                }
            }
            case "timestamp" -> builder.timestamp(value);
            case "username" -> builder.contributor(value);
            case "text" -> builder.text(value);
            default -> {
                //
            }
        }
    }
}


