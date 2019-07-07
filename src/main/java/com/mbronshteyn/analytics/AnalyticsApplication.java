package com.mbronshteyn.analytics;

import lombok.*;
import lombok.extern.java.Log;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
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
          this.pageViewOut.send( pageViewEventMessage );
          log.info(  "sent: " + pageViewEventMessage.toString() );
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
    public void process ( @Input ( AnaylyticsBinding.PAGE_VIEWS_IN ) KStream<String,PageViewEvent >  eventKStream  ){
      log.info( "inside page event processor" );
      eventKStream
        .foreach((key, value) -> {
          log.info("Consumer: " + value.toString());
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
  String PAGE_COUNT_MV = "pcmv";
  String PAGE_COUNT_OUT = "pcout";
  String PAGE_COUNT_IN = "pcin";

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
