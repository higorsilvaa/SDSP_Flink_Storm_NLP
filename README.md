### NLP Pipeline
    - Select relevant JSON fields from JSON file;
    - Identify tweet language (English only);
    - Identify **JJ** (adjectives) using Part of Speech (POS pre-trained model);
    - Identify **PERSON** names using Named Entity Recognition (NER pre-trained model);

### Twitter Search
    - Search keys that were used: "War", "Putin", "Ukraine", "Russia", "Zelensky", "USA", "United States", "U.S.", "U.S.A.".

### Proposal 

In this work, we process tweets about the war in Ukraine to identify the which adjectives are used to describe the main people involved in the war, like Putin, Zelensky and Biden.
To do that, Flink API was used to get the tweets in real time, in other words, stream processing.
In order to deal with the text from tweets, Natural Language Processing (NLP) pipeline was used to clear, select person names as adjectives.
To analyze the Flink performamce, latency and throughput was colected.
As a result, it's provide insights about the sentiment of war and about the people on it.

### About the application ...

The application was made in Java, and to generate the analysis about NLP and FLink performance was used Python.
To run the analitic code, just uses jupyter to see the graphs and annotations.
