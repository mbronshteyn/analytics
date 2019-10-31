package com.mbronshteyn.analytics;

import lombok.*;
import lombok.extern.java.Log;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Log
@EnableBinding(AnaylyticsBinding.class)
public class AnalyticsApplication {

  @Component
  public static class PageViewEventSource implements ApplicationRunner {

    private final MessageChannel pageViewOut;

    public PageViewEventSource(AnaylyticsBinding binding) {
      this.pageViewOut = binding.pageViewOut();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

      List<String> names = Arrays.asList("Anna", "Toby", "Lucy", "Mike");
      List<String> pages = Arrays.asList("blog", "about", "login");

      Runnable runnable = () -> {

        String rPage = pages.get( new Random().nextInt( pages.size() ));
        String rName = names.get( new Random().nextInt( names.size() ));

        PageViewEvent pageViewEvent = PageViewEvent.builder()
          .userId( rName )
          .page( rPage )
          .duration( ( Math.random() > .5 ? 10L : 1000L ))
          .build();

        Message<PageViewEvent> pageViewEventMessage = MessageBuilder
          .withPayload(pageViewEvent)
          .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
          .build();

        try {
//          this.pageViewOut.send( pageViewEventMessage );
//          log.info(  "sent: " + pageViewEventMessage.toString() );
        } catch (Exception e) {
          log.info( e.getMessage() );
        }
      };

      Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);

    }
  }

  @Component
  @Log
  public static class PageViewEventProcessor {

    @StreamListener
    public void processPageViewEvent ( @Input ( AnaylyticsBinding.PAGE_VIEWS_IN ) KStream<String,PageViewEvent >  eventKStream  ){
      log.info( "inside page event processor" );
      Offset offset = new Offset();
      eventKStream
        .foreach((key, value) -> {
          log.info("Consumer: " + value.toString());
        });
      log.info( offset.toString() );
    }

    @StreamListener
    public void processArchive ( @Input ( AnaylyticsBinding.ARCHIVE_IN ) KStream<String, ArchiveEvent > eventKStream ){
      log.info( "inside archive in processor" );
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      Offset offset = new Offset();
      eventKStream
              .foreach((key, value) -> {
                try {
                  log.info("Consumer Archive: " + value.getContent());
                  if (value.getPage() == value.getTotalPages()) {
                    // handle case when we have only one page
                    if( value.getPage() == 1 ){
                      outputStream.write( Base64.getDecoder().decode(value.getContent()));
                    }
                    FileOutputStream fos = new FileOutputStream(new File( key + ".gz"));
                    outputStream.writeTo( fos );
                    fos.close();
                    // reset stream to get ready for the next file
                    offset.reset();
                    outputStream.reset();

                    log.info("Total Count Archive: " + offset.toString());
                  } else {
                    byte[] zipContent = Base64.getDecoder().decode(value.getContent());
                    outputStream.write( zipContent, offset.getValue(), zipContent.length  );
                    offset.add( zipContent.length );
                  }
                } catch( Exception e ){

                }
              });
    }
  }



//  @Component
//  @Log
//  public static class PageCountSync {
//    @StreamListener
//    public void process( @Input ( AnaylyticsBinding.PAGE_COUNT_IN ) KTable<String,Long> counts ){
//
//      counts.toStream().foreach( ( key, value ) -> {
//        log.info( "PageCountIn: Key: " + key + " Value: " + value );
//      });
//
//    }
//
//  }

  public static void main(String[] args) {
    SpringApplication.run(AnalyticsApplication.class, args);
  }

}

interface AnaylyticsBinding {

  String PAGE_VIEWS_OUT = "pvout";
  String PAGE_VIEWS_IN = "pvin";
  String ARCHIVE_IN = "archiveIn";

  String PAGE_COUNT_OUT = "pcout";
  String PAGE_COUNT_IN = "pcin";

  @Input( ARCHIVE_IN )
  KStream<String, ArchiveEvent> archiveIn();

  @Input( PAGE_VIEWS_IN )
  KStream<String,PageViewEvent> pageViewsIn();

//  @Input( PAGE_COUNT_IN )
//  KTable<String,Long> pageCountIn();

  @Output(PAGE_VIEWS_OUT)
  MessageChannel pageViewOut();

//  @Output( PAGE_COUNT_OUT )
//  KStream<String,Long> pageCountOut();

}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
class PageViewEvent {
  private String userId, page;
  private Long duration;
}

class ArchiveEvent {
  private Long page, totalPages;
  private String content;

  public long getPage() {
    return page;
  }

  public void setPage(long page) {
    this.page = page;
  }

  public long getTotalPages() {
    return totalPages;
  }

  public void setTotalPages(long totalPages) {
    this.totalPages = totalPages;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }
}

@ToString
class Offset {

  public Offset(){
  }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    private int value = 0;

  public void add( int value ){
    this.value += value;
  }

  public void reset(){
    value = 0;
  }
}
