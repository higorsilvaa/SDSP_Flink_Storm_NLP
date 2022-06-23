package twitter_streaming;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Tweet {
   public static String textFromString(String tweet) { // get the text of tweet from a string Json
        ObjectMapper jsonParser = new ObjectMapper();
        Boolean isEnLang = false;
        JsonNode node, lang;

        try {
            node = jsonParser.readValue(tweet, JsonNode.class);
        } catch (Exception e) {
            return "ParserError";
        }

        lang = node.get("lang");
        try {
            isEnLang = lang.asText().equals("en");
        } catch (NullPointerException e) {
            return "LangNotDefined";
        }

        if (isEnLang)
            return node.get("text").asText();

        return "NotInEnglish";
    }
}