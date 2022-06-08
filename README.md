### NLP Pipeline
    - Select relevant JSON fields from JSON file;
    - Identify tweet language (English only);
    - Remove URLs (news) and stop-words (take care!!! Do not remove "not/no/don't");
    - Identify **PERSON** names using Named Entity Recognition (NER pre-trained model);
        - Identify whatever name along with our search (Will Smith and Chris Rock)
    - Sentiment Analysis (using a pre-trained model for English);
    - Relate names and sentiments (count sentiment related to the person names).
        - E.g.: 
                Will Smith = Positive;
                Will Smith + Jada = Negative;
                Chris Rock + Anyone = Positive.

### Twitter Search
    - Tweets containing "Will Smith" and/or "Chris Rock" names;
    - Tweets from the beginning of Oscar ceremony until 24 hours later.

### Proposal 

In this work, we aim to process tweets from the Oscars 2022 to identify the sentiment expressed about Will Smith and Chris Rock along and after the ceremony.
We aim to compare the use of two different data stream processing systems (Apache Flink and Apache Storm) in the retrieval and processing of tweets from the selected period.
In order to deal with the text from tweets, we aim to build a Natural Language Processing (NLP) pipeline to clear, select person names, and classify the sentiment of tweets.
To compare both systems, we will use metrics, such as latency, throughput, and CPU/memory usage.
As a result, we will provide insights into the usage of each system and how they perform when working with NLP tasks.

### Title

A Flink and Storm Comparison applied to Sentiment Analysis of tweets about the Oscars 2022
