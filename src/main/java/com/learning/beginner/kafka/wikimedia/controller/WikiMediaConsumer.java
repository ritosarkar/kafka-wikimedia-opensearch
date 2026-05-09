package com.learning.beginner.kafka.wikimedia.controller;

import com.learning.beginner.kafka.wikimedia.service.ConsumeDataOpenSearch;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequiredArgsConstructor
public class WikiMediaConsumer {
    public final ConsumeDataOpenSearch consumeDataOpenSearch;

    @PostMapping("/processIndex")
    public void processToOpenSearch() throws IOException {
        consumeDataOpenSearch.consumeData();

    }
}
